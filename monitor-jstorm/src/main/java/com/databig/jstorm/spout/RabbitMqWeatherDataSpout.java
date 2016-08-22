package com.databig.jstorm.spout;

import java.io.IOException;
import java.util.Map;


import com.databig.jstorm.common.RocketMQUtil;
import com.databig.jstorm.common.constant.CommonConstant;
import com.databig.jstorm.common.log.LogConfigUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.QueueingConsumer.Delivery;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
/**
 * 在整个业务流程上，rabbitmq的消费者端即为整个storm topology的spout，
 * @description DMIS 调度数据
 * @author enersun_lhb
 */

public class RabbitMqWeatherDataSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private QueueingConsumer consumer;
	private Delivery delivery;
	protected String id;
	private Channel chan;

	
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		LogConfigUtils.logInfo("init DefaultMQPushConsumer");
		this.collector = collector;
		this.id = context.getThisComponentId() + ":" + context.getThisTaskId();
		LogConfigUtils.logInfo("init channel");
    	chan = RocketMQUtil.getChannel();
    	LogConfigUtils.logInfo("init Consumer ");
	    try {
	    	// 声明转发器 
//	    	chan.exchangeDeclare(EXCHANGE_NAME, "topic");  
	    	// 接收所有与kernel相关的消息  
	        chan.queueBind(CommonConstant.RabbitMqConstant.RECEIVE_QUEUE, CommonConstant.RabbitMqConstant.EXCHANGE_NAME, CommonConstant.RabbitMqConstant.RECEIVE_ROUTY_KEY); 
	    	// 声明消费者  
	    	consumer = new QueueingConsumer(chan);
			chan.basicConsume(CommonConstant.RabbitMqConstant.RECEIVE_QUEUE, true, consumer);//如果是false 需要接受方发送ack回执,删除消息
//            chan.basicQos(1);  //这样RabbitMQ就会使得每个Consumer在同一个时间点最多处理一个Message。换句话说，在接收到该Consumer的ack前，他它不会将新的Message分发给它。 

		} catch (IOException e) {
			e.printStackTrace();
		}
	    LogConfigUtils.logInfo("Successfully init " + id);
	}

	@Override
	public void nextTuple() {
		String message = null;
		// 等待队列推送消息
		try {
			delivery = consumer.nextDelivery();
			String  dataId = delivery.getEnvelope().getRoutingKey();
			while (delivery != null) {
				message = new String(delivery.getBody(), "UTF-8");
				// 反馈给服务器表示收到信息 与channel.BasicConsume("task_queue", false, null,consumer);对应
				// chan.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				collector.emit(new Values(dataId,message));
				Thread.sleep(6000);
			}
		} catch (ShutdownSignalException | ConsumerCancelledException | InterruptedException | IOException e2) {
			e2.printStackTrace();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("dataId","message"));
	}

	/**
     * 启用 ack 机制，详情参考：https://github.com/alibaba/jstorm/wiki/Ack-%E6%9C%BA%E5%88%B6
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    /**
     * 消息处理失败后需要自己处理
     * @param msgId
     */
    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        LogConfigUtils.logInfo("ack fail,msgId"+msgId);
    }

}
