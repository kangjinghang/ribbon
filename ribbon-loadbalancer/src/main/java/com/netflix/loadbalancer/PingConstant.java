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

/**
 * A utility Ping Implementation that returns whatever its been set to return
 * (alive or dead)
 * @author stonse
 *
 */
public class PingConstant implements IPing { // 固定值返回某服务是否可用，默认返回 true，即可用
		boolean constant = true;

		public void setConstant(String constantStr) {
				constant = (constantStr != null) && (constantStr.toLowerCase().equals("true"));
		}

		public void setConstant(boolean constant) {
				this.constant = constant;
		}

		public boolean getConstant() {
				return constant;
		}

		public boolean isAlive(Server server) {
				return constant;
		}
}
