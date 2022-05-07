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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.netflix.client.config.IClientConfig;

/**
 * A rule that uses the a {@link CompositePredicate} to filter servers based on zone and availability. The primary predicate is composed of
 * a {@link ZoneAvoidancePredicate} and {@link AvailabilityPredicate}, with the fallbacks to {@link AvailabilityPredicate}
 * and an "always true" predicate returned from {@link AbstractServerPredicate#alwaysTrue()} 
 * 
 * @author awang
 * 基于区域（zone）和服务有效性原则，使用 CompositePredicate 类来过滤服务列表。CompositePredicate = ZoneAvoidancePredicate + AvailabilityPredicate
 * 使用 ZoneAvoidancePredicate 和 AvailabilityPredicate 来判断是否选择某个 server，前一个，以一个区域为单位考察可用性，对于不可用的区域整个丢弃，从剩下区域中选可用的 server。判断出最差的区域，排除掉最差区域。在剩下的区域中，将按照服务器实例数的概率抽样法选择，从而判断判定一个 zone 的运行性能是否可用，剔除不可用的 zone（的所有server），AvailabilityPredicate 用于过滤掉连接数过多的 Server
 */
public class ZoneAvoidanceRule extends PredicateBasedRule { // 根据 Server 的 zone 区域和可用性来轮询选择

    private static final Random random = new Random();
    // 使用了 CompositePredicate 来进行服务实例清单的过滤
    private CompositePredicate compositePredicate;
    
    public ZoneAvoidanceRule() {
        ZoneAvoidancePredicate zonePredicate = new ZoneAvoidancePredicate(this);
        AvailabilityPredicate availabilityPredicate = new AvailabilityPredicate(this);
        compositePredicate = createCompositePredicate(zonePredicate, availabilityPredicate);
    }
    // 组合过滤条件，在其构造函数中，它以 ZoneAvoidancePredicate 为主过滤条件，AvailabilityPredicate 为次过滤条件初始化了组合过滤条件的实例。
    private CompositePredicate createCompositePredicate(ZoneAvoidancePredicate p1, AvailabilityPredicate p2) {
        return CompositePredicate.withPredicates(p1, p2)
                             .addFallbackPredicate(p2)
                             .addFallbackPredicate(AbstractServerPredicate.alwaysTrue())
                             .build();
    }

    @Override
    public void initWithNiwsConfig(IClientConfig clientConfig) {
        ZoneAvoidancePredicate zonePredicate = new ZoneAvoidancePredicate(this, clientConfig);
        AvailabilityPredicate availabilityPredicate = new AvailabilityPredicate(this, clientConfig);
        compositePredicate = createCompositePredicate(zonePredicate, availabilityPredicate);
    }

    static Map<String, ZoneSnapshot> createSnapshot(LoadBalancerStats lbStats) {
        Map<String, ZoneSnapshot> map = new HashMap<String, ZoneSnapshot>();
        for (String zone : lbStats.getAvailableZones()) {
            ZoneSnapshot snapshot = lbStats.getZoneSnapshot(zone);
            map.put(zone, snapshot);
        }
        return map;
    }

    static String randomChooseZone(Map<String, ZoneSnapshot> snapshot,
            Set<String> chooseFrom) {
        if (chooseFrom == null || chooseFrom.size() == 0) {
            return null;
        }
        String selectedZone = chooseFrom.iterator().next();
        if (chooseFrom.size() == 1) {
            return selectedZone;
        }
        int totalServerCount = 0;
        for (String zone : chooseFrom) {
            totalServerCount += snapshot.get(zone).getInstanceCount();
        }
        int index = random.nextInt(totalServerCount) + 1;
        int sum = 0;
        for (String zone : chooseFrom) {
            sum += snapshot.get(zone).getInstanceCount();
            if (index <= sum) {
                selectedZone = zone;
                break;
            }
        }
        return selectedZone;
    }
    // 获取可用的 Zone 区域集合。通过 Zone 区域快照中的统计数据来实现可用区的挑选。
    public static Set<String> getAvailableZones(
            Map<String, ZoneSnapshot> snapshot, double triggeringLoad, // triggeringLoad 默认 0.2。triggeringBlackoutPercentage 默认 0.99999
            double triggeringBlackoutPercentage) {
        if (snapshot.isEmpty()) { // 剔除所属实例数为零的Zone区域
            return null;
        }
        Set<String> availableZones = new HashSet<String>(snapshot.keySet());
        if (availableZones.size() == 1) {
            return availableZones;
        }
        Set<String> worstZones = new HashSet<String>();
        double maxLoadPerServer = 0;
        boolean limitedZoneAvailability = false;

        for (Map.Entry<String, ZoneSnapshot> zoneEntry : snapshot.entrySet()) {
            String zone = zoneEntry.getKey();
            ZoneSnapshot zoneSnapshot = zoneEntry.getValue();
            int instanceCount = zoneSnapshot.getInstanceCount();
            if (instanceCount == 0) {
                availableZones.remove(zone);
                limitedZoneAvailability = true;
            } else {
                double loadPerServer = zoneSnapshot.getLoadPerServer();
                if (((double) zoneSnapshot.getCircuitTrippedCount())
                        / instanceCount >= triggeringBlackoutPercentage // 剔除实例故障率（断路器断开次数/实例数）大于等于阈值（默认为0.99999）的 Zone 区域
                        || loadPerServer < 0) { // 剔除实例平均负载小于零的 Zone 区域
                    availableZones.remove(zone);
                    limitedZoneAvailability = true;
                } else {
                    if (Math.abs(loadPerServer - maxLoadPerServer) < 0.000001d) { // 根据 Zone 区域的实例平均负载来计算出最差的 Zone 区域，这里的最差指的是实例平均负载最高的 Zone 区域
                        // they are the same considering double calculation
                        // round error
                        worstZones.add(zone);
                    } else if (loadPerServer > maxLoadPerServer) {
                        maxLoadPerServer = loadPerServer;
                        worstZones.clear();
                        worstZones.add(zone);
                    }
                }
            }
        }

        if (maxLoadPerServer < triggeringLoad && !limitedZoneAvailability) {
            // zone override is not needed here
            return availableZones; // 如果在上面的过程中没有符合剔除要求的区域，同时实例最大平均负载小于阈值（默认为20%），就直接返回所有 Zone 区域为可用区域
        }
        String zoneToAvoid = randomChooseZone(snapshot, worstZones); // 否则，从最坏 Zone 区域集合中随机的选择一个，将它从可用 Zone 区域集合中剔除。
        if (zoneToAvoid != null) {
            availableZones.remove(zoneToAvoid);
        }
        return availableZones;

    }

    public static Set<String> getAvailableZones(LoadBalancerStats lbStats,
            double triggeringLoad, double triggeringBlackoutPercentage) {
        if (lbStats == null) {
            return null;
        }
        Map<String, ZoneSnapshot> snapshot = createSnapshot(lbStats);
        return getAvailableZones(snapshot, triggeringLoad,
                triggeringBlackoutPercentage);
    }

    @Override
    public AbstractServerPredicate getPredicate() {
        return compositePredicate;
    }    
}
