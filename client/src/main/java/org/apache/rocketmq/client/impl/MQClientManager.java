/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

/**
 * Client管理器
 *
 * 一个JVM中，Rocket的所有Client（包括Producer和Consumer）都归 MQClientManager 管理。由它统一创建底层的MQClientInstance。
 *
 * 它是单例的。
 * 一个JVM中，只会有一个MQClientManager。
 * 一个MQClientManager（单例）下可以有多个MQClientInstance，用实例名区分（其实主要是网卡IP），一般其实只有一个。
 * 一个MQClientInstance下可以有多个Producer、Consumer，用Group来区分。
 *
 */
public class MQClientManager {
    private final static InternalLogger log = ClientLogger.getLog();
    /**
     * 饿汉模式的单例。
     * 而且是类变量，类加载的时候就创建了。
     */
    private static MQClientManager instance = new MQClientManager();
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();
    // 管理MQClientInstance的容器。因为有多线程访问的情况，所以，使用并发类容器 ConcurrentHashMap
    private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable =
        new ConcurrentHashMap<String, MQClientInstance>();

    /**
     * 一看私有化构造器就知道是 单例模式
     */
    private MQClientManager() {

    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig) {
        return getOrCreateMQClientInstance(clientConfig, null);
    }

    /**
     * 基本都是在具体的client实例（包括Producer和Client）的start方法中被调用
     */
    public MQClientInstance getOrCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        // 不管是Producer还是Consumer，clientId都是 [IP+实例名（Instance Name）]
        String clientId = clientConfig.buildMQClientId();
        MQClientInstance instance = this.factoryTable.get(clientId);
        if (null == instance) {
            instance =
                new MQClientInstance(clientConfig.cloneClientConfig(), this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
