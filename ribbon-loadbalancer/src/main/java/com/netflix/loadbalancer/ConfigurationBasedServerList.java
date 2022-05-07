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

import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.netflix.client.config.CommonClientConfigKey;
import com.netflix.client.config.IClientConfig;

/**
 * Utility class that can load the List of Servers from a Configuration (i.e
 * properties available via Archaius). The property name be defined in this format:
 * 
 * <pre>{@code
<clientName>.<nameSpace>.listOfServers=<comma delimited hostname:port strings>
}</pre>
 * 
 * @author awang
 * 
 */
public class ConfigurationBasedServerList extends AbstractServerList<Server>  { // 通过配置文件获取服务列表

	private IClientConfig clientConfig;
		
	@Override
	public List<Server> getInitialListOfServers() {
	    return getUpdatedListOfServers();
	}
	// 从配置文件中获取服务列表，即通过 *.ribbon.listOfServers 配置的静态服务列表
	@Override
	public List<Server> getUpdatedListOfServers() {
        String listOfServers = clientConfig.get(CommonClientConfigKey.ListOfServers);
        return derive(listOfServers);
	}

	@Override
	public void initWithNiwsConfig(IClientConfig clientConfig) {
	    this.clientConfig = clientConfig;
	}
	
	protected List<Server> derive(String value) {
	    List<Server> list = Lists.newArrayList();
		if (!Strings.isNullOrEmpty(value)) {
			for (String s: value.split(",")) {
				list.add(new Server(s.trim()));
			}
		}
        return list;
	}

	@Override
	public String toString() {
		return "ConfigurationBasedServerList:" + getUpdatedListOfServers();
	}
}
