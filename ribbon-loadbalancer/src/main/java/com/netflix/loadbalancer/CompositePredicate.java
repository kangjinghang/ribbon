
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

import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

/**
 * A predicate that is composed from one or more predicates in "AND" relationship.
 * It also has the functionality of "fallback" to one of more different predicates.
 * If the primary predicate yield too few filtered servers from the {@link #getEligibleServers(List, Object)}
 * API, it will try the fallback predicates one by one, until the number of filtered servers
 * exceeds certain number threshold or percentage threshold. 
 * 
 * @author awang
 *
 */
public class CompositePredicate extends AbstractServerPredicate {
    // 主过滤条件
    private AbstractServerPredicate delegate;
    // 次过滤条件列表。次过滤列表是可以拥有多个的，并且由于它采用了 List 存储所以次过滤条件是按顺序执行的
    private List<AbstractServerPredicate> fallbacks = Lists.newArrayList();
        
    private int minimalFilteredServers = 1;
    
    private float minimalFilteredPercentage = 0;    
    
    @Override
    public boolean apply(@Nullable PredicateKey input) {
        return delegate.apply(input);
    }

    
    public static class Builder {
        
        private CompositePredicate toBuild;
        
        Builder(AbstractServerPredicate primaryPredicate) {
            toBuild = new CompositePredicate();    
            toBuild.delegate = primaryPredicate;                    
        }

        Builder(AbstractServerPredicate ...primaryPredicates) {
            toBuild = new CompositePredicate();
            Predicate<PredicateKey> chain = Predicates.<PredicateKey>and(primaryPredicates);
            toBuild.delegate =  AbstractServerPredicate.ofKeyPredicate(chain);                
        }

        public Builder addFallbackPredicate(AbstractServerPredicate fallback) {
            toBuild.fallbacks.add(fallback);
            return this;
        }
                
        public Builder setFallbackThresholdAsMinimalFilteredNumberOfServers(int number) {
            toBuild.minimalFilteredServers = number;
            return this;
        }
        
        public Builder setFallbackThresholdAsMinimalFilteredPercentage(float percent) {
            toBuild.minimalFilteredPercentage = percent;
            return this;
        }
        
        public CompositePredicate build() {
            return toBuild;
        }
    }
    
    public static Builder withPredicates(AbstractServerPredicate ...primaryPredicates) {
        return new Builder(primaryPredicates);
    }

    public static Builder withPredicate(AbstractServerPredicate primaryPredicate) {
        return new Builder(primaryPredicate);
    }

    /**
     * Get the filtered servers from primary predicate, and if the number of the filtered servers
     * are not enough, trying the fallback predicates  
     */
    @Override // 使用主过滤条件对所有实例过滤并返回过滤后的实例清单
    public List<Server> getEligibleServers(List<Server> servers, Object loadBalancerKey) {
        List<Server> result = super.getEligibleServers(servers, loadBalancerKey);
        Iterator<AbstractServerPredicate> i = fallbacks.iterator(); // 每次过滤之后（包括主过滤条件和次过滤条件），都需要判断下面两个条件，只要有一个符合就不再进行过滤，将当前结果返回供线性轮询算法选择：
        while (!(result.size() >= minimalFilteredServers && result.size() > (int) (servers.size() * minimalFilteredPercentage)) // 过滤后的实例总数 >= 最小过滤实例数（minimalFilteredServers，默认为1）&& 过滤后的实例比例 > 最小过滤百分比（minimalFilteredPercentage，默认为0）
                && i.hasNext()) { // 依次使用次过滤条件列表中的过滤条件对主过滤条件的结果进行过滤
            AbstractServerPredicate predicate = i.next();
            result = predicate.getEligibleServers(servers, loadBalancerKey);
        }
        return result;
    }
}
