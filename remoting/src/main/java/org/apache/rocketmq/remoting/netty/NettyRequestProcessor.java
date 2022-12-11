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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Common remoting command processor
 *
 * 统一的Netty请求处理器-抽象接口
 *
 */
public interface NettyRequestProcessor {

    /**
     * 在这里就可以看到，同步和异步接口的设计不同。
     *
     * 这里是同步处理请求，所以，会有返回值。因为要求同步返回嘛。
     * 但是看实现了这个接口的异步抽象类，扩展的异步方法。就不需要返回值了，直接是void。
     *
     * 但是异步任务的处理结果，需要做处理。所以，需要设计一个策略回调接口，作为方法参数。
     * 允许自定义异步结果处理逻辑。（这就是扩展性）
     *
     * 这种，同步、异步的设计方式，我觉得也是挺通用的。
     *
     * @see AsyncNettyRequestProcessor#asyncProcessRequest(ChannelHandlerContext, RemotingCommand, RemotingResponseCallback)
     *
     */
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception;

    boolean rejectRequest();

}
