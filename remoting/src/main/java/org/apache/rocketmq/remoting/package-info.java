package org.apache.rocketmq.remoting;

/*
 *
 * 无论是Producer/Consumer（统一为Client），还是Broker，最终的数据肯定是要走的网络。
 * 因此，RocketMQ也抽象出了一个网络层，就是这个remoting项目和remoting包。
 *
 */