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

import com.google.common.base.Optional;

/**
 * A rule which delegates the server filtering logic to an instance of {@link AbstractServerPredicate}.
 * After filtering, a server is returned from filtered list in a round robin fashion.
 * 
 * 
 * @author awang
 * 这是一个抽象策略，它也继承了ClientConfigEnabledRoundRobinRule，从其命名中可以猜出他是一个基于Predicate实现的策略，Predicate是Google Guava Collection工具对集合进行过滤的条件接口。
 */
public abstract class PredicateBasedRule extends ClientConfigEnabledRoundRobinRule { // 代理 AbstractServerPredicate 类的实例，chooseRoundRobinAfterFiltering 后获取 Server 实例
   
    /**
     * Method that provides an instance of {@link AbstractServerPredicate} to be used by this class.
     * 
     */
    public abstract AbstractServerPredicate getPredicate(); // 定义了抽象方法 getPredicate 来获取 AbstractServerPredicate 对象的实现
        
    /**
     * Get a server by calling {@link AbstractServerPredicate#chooseRandomlyAfterFiltering(java.util.List, Object)}.
     * The performance for this method is O(n) where n is number of servers to be filtered.
     */
    @Override
    public Server choose(Object key) {
        ILoadBalancer lb = getLoadBalancer();
        Optional<Server> server = getPredicate().chooseRoundRobinAfterFiltering(lb.getAllServers(), key); // 选出具体的服务实例
        if (server.isPresent()) {
            return server.get();
        } else {
            return null;
        }       
    }
}
