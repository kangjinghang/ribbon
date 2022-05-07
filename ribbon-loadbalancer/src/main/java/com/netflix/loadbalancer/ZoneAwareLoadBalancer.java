/*
*
* Copyright 2013 Netflix, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/
package com.netflix.loadbalancer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.netflix.client.ClientFactory;
import com.netflix.client.config.ClientConfigFactory;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;
import com.netflix.client.config.IClientConfigKey;
import com.netflix.client.config.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Load balancer that can avoid a zone as a whole when choosing server. 
 *<p>
 * The key metric used to measure the zone condition is Average Active Requests,
which is aggregated per rest client per zone. It is the
total outstanding requests in a zone divided by number of available targeted instances (excluding circuit breaker tripped instances).
This metric is very effective when timeout occurs slowly on a bad zone.
<p>
The  LoadBalancer will calculate and examine zone stats of all available zones. If the Average Active Requests for any zone has reached a configured threshold, this zone will be dropped from the active server list. In case more than one zone has reached the threshold, the zone with the most active requests per server will be dropped.
Once the the worst zone is dropped, a zone will be chosen among the rest with the probability proportional to its number of instances.
A server will be returned from the chosen zone with a given Rule (A Rule is a load balancing strategy, for example {@link AvailabilityFilteringRule})
For each request, the steps above will be repeated. That is to say, each zone related load balancing decisions are made at real time with the up-to-date statistics aiding the choice.

 * @author awang
 *
 * @param <T>
 */ // 重写 DynamicServerListLoadBalancer 中的 chooseServer 方法，由于DynamicServerListLoadBalancer 中负责均衡的策略依然是 BaseLoadBalancer 中的线性轮询策略，这种策略不具备区域感知功能
    // 该算法实现简单并没有区域（Zone）的概念，所以它会把所有实例视为一个Zone下的节点来看待，这样就会周期性的产生跨区域（Zone）访问的情况，由于跨区域会产生更高的延迟，这些实例主要以防止区域性故障实现高可用为目的而不能作为常规访问的实例，所以在多区域部署的情况下会有一定的性能问题
public class ZoneAwareLoadBalancer<T extends Server> extends DynamicServerListLoadBalancer<T> { // 区域感知的 lb。ZoneAwareLoadBalancer 主要扩展的功能就是增加了 zone 区域过滤
    // 1､重写setServerListForZones(Map)方法，按zone区域创建BaseLoadBalancer，如果ZoneAwareLoadBalancer#IRule为空，默认使用AvailabilityFilteringRule，否则就使用ZoneAwareLoadBalancer#IRule。
    // 2､重写chooseServer(Object)方法，当负载均衡器中维护的实例zone区域个数大于1时，会执行以下策略，否则执行父类的方法。
    // 3､根据LoadBalancerStats创建zone区域快照用于后续的算法中。
    // 4､根据zone区域快照中的统计数据来实现可用区的挑选。
    // 5､当返回的可用zone区域集合不空，并且个数小于zone区域总数，就随机选择一个zone区域。
    // 6､在确定了zone区域后，获取对应zone区域的负载均衡器，并调用chooseServer(Object)方法来选择具体的服务实例。
    // 统计每个zone的平均请求的情况，保证从所有zone选取对当前客户端服务最好的服务组列表
    // 这是一个区域内的未完成请求的总数除以可用的目标实例数量(不包括断路器跳闸实例)。当超时在一个坏的区域中缓慢发生时，这个度量是非常有效的
    // 负载均衡器将计算和检查所有可用区域的区域数据。如果任何区域的平均主动请求达到了配置的阈值，则该区域将从活动服务器列表中删除。如果超过一个区域已经达到了阈值，那么每个服务器上最活跃请求的区域将被删除。一旦最坏的区域被放弃，一个区域将被选择在剩下的区域中，与它的实例数量成正比。对于每个请求，将从所选择的区域返回一个服务器(规则是负载均衡策略，例如{可用性过滤规则})，上面的步骤将被重复。也就是说，每个区域的相关负载均衡决策都是在实时统计数据的帮助下做出的
    private ConcurrentHashMap<String, BaseLoadBalancer> balancers = new ConcurrentHashMap<String, BaseLoadBalancer>(); // balancers 对象，它将用来存储每个 Zone 区域对应的负载均衡器
    
    private static final Logger logger = LoggerFactory.getLogger(ZoneAwareLoadBalancer.class);

    private static final IClientConfigKey<Boolean> ENABLED = new CommonClientConfigKey<Boolean>(
            "ZoneAwareNIWSDiscoveryLoadBalancer.enabled", true){};

    private static final IClientConfigKey<Double> TRIGGERING_LOAD_PER_SERVER_THRESHOLD = new CommonClientConfigKey<Double>(
            "ZoneAwareNIWSDiscoveryLoadBalancer.%s.triggeringLoadPerServerThreshold", 0.2d){};

    private static final IClientConfigKey<Double> AVOID_ZONE_WITH_BLACKOUT_PERCENTAGE = new CommonClientConfigKey<Double>(
            "ZoneAwareNIWSDiscoveryLoadBalancer.%s.avoidZoneWithBlackoutPercetage", 0.99999d){};


    private Property<Double> triggeringLoad = Property.of(TRIGGERING_LOAD_PER_SERVER_THRESHOLD.defaultValue());

    private Property<Double> triggeringBlackoutPercentage = Property.of(AVOID_ZONE_WITH_BLACKOUT_PERCENTAGE.defaultValue());

    private Property<Boolean> enabled = Property.of(ENABLED.defaultValue());

    void setUpServerList(List<Server> upServerList) {
        this.upServerList = upServerList;
    }

    @Deprecated
    public ZoneAwareLoadBalancer(IClientConfig clientConfig, IRule rule,
            IPing ping, ServerList<T> serverList, ServerListFilter<T> filter) {
        super(clientConfig, rule, ping, serverList, filter);

        String name = Optional.ofNullable(getName()).orElse("default");

        this.enabled = clientConfig.getGlobalProperty(ENABLED);
        this.triggeringLoad = clientConfig.getGlobalProperty(TRIGGERING_LOAD_PER_SERVER_THRESHOLD.format(name));
        this.triggeringBlackoutPercentage = clientConfig.getGlobalProperty(AVOID_ZONE_WITH_BLACKOUT_PERCENTAGE.format(name));
    }

    public ZoneAwareLoadBalancer(IClientConfig clientConfig, IRule rule,
                                 IPing ping, ServerList<T> serverList, ServerListFilter<T> filter,
                                 ServerListUpdater serverListUpdater) {
        super(clientConfig, rule, ping, serverList, filter, serverListUpdater);

        String name = Optional.ofNullable(getName()).orElse("default");

        this.enabled = clientConfig.getGlobalProperty(ENABLED);
        this.triggeringLoad = clientConfig.getGlobalProperty(TRIGGERING_LOAD_PER_SERVER_THRESHOLD.format(name));
        this.triggeringBlackoutPercentage = clientConfig.getGlobalProperty(AVOID_ZONE_WITH_BLACKOUT_PERCENTAGE.format(name));
    }

    public ZoneAwareLoadBalancer() {
        super();
    }

    @Override
    public void initWithNiwsConfig(IClientConfig clientConfig) {
        super.initWithNiwsConfig(clientConfig);

        String name = Optional.ofNullable(getName()).orElse("default");

        this.enabled = clientConfig.getGlobalProperty(ENABLED);
        this.triggeringLoad = clientConfig.getGlobalProperty(TRIGGERING_LOAD_PER_SERVER_THRESHOLD.format(name));
        this.triggeringBlackoutPercentage = clientConfig.getGlobalProperty(AVOID_ZONE_WITH_BLACKOUT_PERCENTAGE.format(name));
    }

    @Override
    protected void setServerListForZones(Map<String, List<Server>> zoneServersMap) {
        super.setServerListForZones(zoneServersMap);
        if (balancers == null) {
            balancers = new ConcurrentHashMap<String, BaseLoadBalancer>(); // 创建了一个 ConcurrentHashMap() 类型的 balancers 对象，它将用来存储每个 Zone 区域对应的负载均衡器
        }
        for (Map.Entry<String, List<Server>> entry: zoneServersMap.entrySet()) {
        	String zone = entry.getKey().toLowerCase();
            getLoadBalancer(zone).setServersList(entry.getValue()); // 具体的负载均衡器的创建，在创建完负载均衡器后又马上调用 setServersList 方法为其设置对应 Zone 区域的实例清单
        }
        // check if there is any zone that no longer has a server
        // and set the list to empty so that the zone related metrics does not
        // contain stale data
        for (Map.Entry<String, BaseLoadBalancer> existingLBEntry: balancers.entrySet()) { // 对 Zone 区域中实例清单的检查
            if (!zoneServersMap.keySet().contains(existingLBEntry.getKey())) { // 看看是否有Zone区域下已经没有实例了
                existingLBEntry.getValue().setServersList(Collections.emptyList()); // 是的话就将 balancers 中对应 Zone 区域的实例列表清空，该操作的作用是为了后续选择节点时，防止过时的 Zone 区域统计信息干扰具体实例的选择算法
            }
        }
    }    
    // 挑选服务实例，来实现对区域的识别的
    @Override
    public Server chooseServer(Object key) {
        if (!enabled.getOrDefault() || getLoadBalancerStats().getAvailableZones().size() <= 1) {  // 只有当负载均衡器中维护的实例所属 Zone 区域个数大于1的时候才会执行这里的选择策略，否则还是将使用父类的实现
            logger.debug("Zone aware logic disabled or there is only one zone");
            return super.chooseServer(key);
        }
        Server server = null;
        try {
            LoadBalancerStats lbStats = getLoadBalancerStats();
            Map<String, ZoneSnapshot> zoneSnapshot = ZoneAvoidanceRule.createSnapshot(lbStats);  // 为当前负载均衡器中所有的 Zone 区域分别创建快照，保存在 Map zoneSnapshot 中，这些快照中的数据将用于后续的算法
            logger.debug("Zone snapshots: {}", zoneSnapshot);
            Set<String> availableZones = ZoneAvoidanceRule.getAvailableZones(zoneSnapshot, triggeringLoad.getOrDefault(), triggeringBlackoutPercentage.getOrDefault()); // 获取可用的 Zone 区域集合
            logger.debug("Available zones: {}", availableZones);
            if (availableZones != null &&  availableZones.size() < zoneSnapshot.keySet().size()) { // 当获得的可用 Zone 区域集合不为空，并且个数小于 Zone 区域总数，就随机的选择一个 Zone 区域。
                String zone = ZoneAvoidanceRule.randomChooseZone(zoneSnapshot, availableZones); // 随机的选择一个 Zone 区域
                logger.debug("Zone chosen: {}", zone);
                if (zone != null) {
                    BaseLoadBalancer zoneLoadBalancer = getLoadBalancer(zone); // 在确定了某个 Zone 区域后，则获取对应 Zone 区域的服务均衡器
                    server = zoneLoadBalancer.chooseServer(key); // 调用 chooseServer 来选择具体的服务实例。在 chooseServer 中将使用 IRule 接口的 choose 方法来选择具体的服务实例。在这里 IRule 接口的实现会使用 ZoneAvoidanceRule 来挑选出具体的服务实例
                }
            }
        } catch (Exception e) {
            logger.error("Error choosing server using zone aware logic for load balancer={}", name, e);
        }
        if (server != null) {
            return server;
        } else {
            logger.debug("Zone avoidance logic is not invoked.");
            return super.chooseServer(key);
        }
    }
     
    @VisibleForTesting
    BaseLoadBalancer getLoadBalancer(String zone) {
        zone = zone.toLowerCase();
        BaseLoadBalancer loadBalancer = balancers.get(zone);
        if (loadBalancer == null) { // 具体的负载均衡器的创建
        	// We need to create rule object for load balancer for each zone
        	IRule rule = cloneRule(this.getRule()); // 在创建负载均衡器的时候会创建它的规则（如果当前实现中没有 IRULE 的实例，就创建一个 AvailabilityFilteringRule 规则；如果已经有具体实例，就clone一个）
            loadBalancer = new BaseLoadBalancer(this.getName() + "_" + zone, rule, this.getLoadBalancerStats());
            BaseLoadBalancer prev = balancers.putIfAbsent(zone, loadBalancer);
            if (prev != null) {
            	loadBalancer = prev;
            }
        } 
        return loadBalancer;        
    }

    private IRule cloneRule(IRule toClone) {
    	IRule rule;
    	if (toClone == null) {
    		rule = new AvailabilityFilteringRule();
    	} else {
    		String ruleClass = toClone.getClass().getName();        		
    		try {
				rule = (IRule) ClientFactory.instantiateInstanceWithClientConfig(ruleClass, this.getClientConfig());
			} catch (Exception e) {
				throw new RuntimeException("Unexpected exception creating rule for ZoneAwareLoadBalancer", e);
			}
    	}
    	return rule;
    }
    
       
    @Override
    public void setRule(IRule rule) {
        super.setRule(rule);
        if (balancers != null) {
            for (String zone: balancers.keySet()) {
                balancers.get(zone).setRule(cloneRule(rule));
            }
        }
    }
}
