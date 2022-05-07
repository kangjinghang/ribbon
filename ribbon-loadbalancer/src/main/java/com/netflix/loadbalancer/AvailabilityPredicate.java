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

import com.google.common.base.Preconditions;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;
import com.netflix.client.config.IClientConfigKey;
import com.netflix.client.config.Property;

import javax.annotation.Nullable;

/**
 * Predicate with the logic of filtering out circuit breaker tripped servers and servers 
 * with too many concurrent connections from this client.
 * 
 * @author awang
 *
 */
public class AvailabilityPredicate extends  AbstractServerPredicate {

    private static final IClientConfigKey<Boolean> FILTER_CIRCUIT_TRIPPED = new CommonClientConfigKey<Boolean>(
            "niws.loadbalancer.availabilityFilteringRule.filterCircuitTripped", true) {};

    private static final IClientConfigKey<Integer> DEFAULT_ACTIVE_CONNECTIONS_LIMIT = new CommonClientConfigKey<Integer>(
            "niws.loadbalancer.availabilityFilteringRule.activeConnectionsLimit", -1) {};

    private static final IClientConfigKey<Integer> ACTIVE_CONNECTIONS_LIMIT = new CommonClientConfigKey<Integer>(
            "ActiveConnectionsLimit", -1) {};

    private Property<Boolean> circuitBreakerFiltering = Property.of(FILTER_CIRCUIT_TRIPPED.defaultValue());
    private Property<Integer> defaultActiveConnectionsLimit = Property.of(DEFAULT_ACTIVE_CONNECTIONS_LIMIT.defaultValue());
    private Property<Integer> activeConnectionsLimit = Property.of(ACTIVE_CONNECTIONS_LIMIT.defaultValue());

    public AvailabilityPredicate(IRule rule, IClientConfig clientConfig) {
        super(rule);
        initDynamicProperty(clientConfig);
    }

    public AvailabilityPredicate(LoadBalancerStats lbStats, IClientConfig clientConfig) {
        super(lbStats);
        initDynamicProperty(clientConfig);
    }

    AvailabilityPredicate(IRule rule) {
        super(rule);
    }

    private void initDynamicProperty(IClientConfig clientConfig) {
        if (clientConfig != null) {
            this.circuitBreakerFiltering = clientConfig.getGlobalProperty(FILTER_CIRCUIT_TRIPPED);
            this.defaultActiveConnectionsLimit = clientConfig.getGlobalProperty(DEFAULT_ACTIVE_CONNECTIONS_LIMIT);
            this.activeConnectionsLimit = clientConfig.getDynamicProperty(ACTIVE_CONNECTIONS_LIMIT);
        }
    }

    private int getActiveConnectionsLimit() {
        Integer limit = activeConnectionsLimit.getOrDefault();
        if (limit == -1) {
            limit = defaultActiveConnectionsLimit.getOrDefault();
            if (limit == -1) {
                limit = Integer.MAX_VALUE;
            }
        }
        return limit;
    }
    // 它并没有像父类中那样，先遍历所有的节点进行过滤，然后在过滤后的集合中选择实例。而是先线性的方式选择一个实例，接着用过滤条件来判断该实例是否满足要求，若满足就直接使用该实例，若不满足要求就再选择下一个实例，并检查是否满足要求，如此循环进行，当这个过程重复了10次还是没有找到符合要求的实例，就采用父类的实现方案。
    @Override // 简单的说，该策略通过线性抽样的方式直接尝试寻找可用且较空闲的实例来使用，优化了父类每次都要遍历所有实例的开销。
    public boolean apply(@Nullable PredicateKey input) {
        LoadBalancerStats stats = getLBStats();
        if (stats == null) {
            return true;
        }
        return !shouldSkipServer(stats.getSingleServerStat(input.getServer()));
    }
    
    private boolean shouldSkipServer(ServerStats stats) {
        if ((circuitBreakerFiltering.getOrDefault() && stats.isCircuitBreakerTripped()) // 是否故障，即断路器是否生效已断开
                || stats.getActiveRequestsCount() >= getActiveConnectionsLimit()) { // 实例的并发请求数大于阈值，默认值为 2^31 - 1，该配置我们可通过参数 ActiveConnectionsLimit 来修改 其中只要有一个满足 apply 就返回false（代表该节点可能存在故障或负载过高），都不满足就返回 true
            return true;
        }
        return false;
    }

}
