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

package com.netflix.ribbon.examples.restclient;

import java.net.URI;

import com.netflix.client.ClientFactory;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpResponse;
import com.netflix.config.ConfigurationManager;
import com.netflix.loadbalancer.ZoneAwareLoadBalancer;
import com.netflix.niws.client.http.RestClient;

public class SampleApp { // 标准样例
	public static void main(String[] args) throws Exception {
        ConfigurationManager.loadPropertiesFromResources("sample-client.properties");  // 1 相关数据配置在config文件中，通过 Archaius ConfigurationManager 加载配置数据。
        System.out.println(ConfigurationManager.getConfigInstance().getProperty("sample-client.ribbon.listOfServers"));
        RestClient client = (RestClient) ClientFactory.getNamedClient("sample-client");  // 2 通过ClientFactory创建RestClient 和 ZoneAwareLoadBalancer（负载均衡器）
        HttpRequest request = HttpRequest.newBuilder().uri(new URI("/")).build(); // 3 使用构建器构建http请求。请注意，我们只提供URI的路径部分（“/”）。一旦服务器被 ZoneAwareLoadBalancer（负载均衡器）选中，完整的请求链接将由RestClient计算。
        for (int i = 0; i < 20; i++)  {
        	HttpResponse response = client.executeWithLoadBalancer(request); // 4 发送请求是通过 RestClient 的 executeWithLoadBalancer() 方法触发的
        	System.out.println("Status code for " + response.getRequestedURI() + "  :" + response.getStatus());
        }
        @SuppressWarnings("rawtypes")
        ZoneAwareLoadBalancer lb = (ZoneAwareLoadBalancer) client.getLoadBalancer();
        System.out.println(lb.getLoadBalancerStats());
        ConfigurationManager.getConfigInstance().setProperty(
        		"sample-client.ribbon.listOfServers", "www.linkedin.com:80,www.google.com:80"); // 5 可以通过修改配置文件来动态的修改可用的服务的列表
        System.out.println("changing servers ...");
        Thread.sleep(3000); // 6
        for (int i = 0; i < 20; i++)  {
            HttpResponse response = null;
            try {
        	    response = client.executeWithLoadBalancer(request);
        	    System.out.println("Status code for " + response.getRequestedURI() + "  : " + response.getStatus());
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }
        System.out.println(lb.getLoadBalancerStats()); // 7
	}
}
