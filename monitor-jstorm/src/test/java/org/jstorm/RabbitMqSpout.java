package org.jstorm;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databig.jstorm.common.RocketMQUtil;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RabbitMqSpout extends BaseRichSpout  {

	/**
	 * Hello world!
	 * http://www.tuicool.com/articles/veA7nu
	 * http://www.tuicool.com/articles/NzyqAn
	 * http://blog.csdn.net/u010220089/article/details/48751427
	 * http://www.cnblogs.com/luxiaoxun/p/3918054.html
	 * http://www.tuicool.com/articles/zuQzum
    */
	private static final long serialVersionUID = 1L;
	private static Logger LOG = LoggerFactory.getLogger(RabbitMqSpout.class);
	private SpoutOutputCollector collector;
	private QueueingConsumer consumer;
	private Delivery delivery;
	protected String id;
	private Channel chan;

	private static final String EXCHANGE_NAME = "DC.Exchange";  
	private static final String RECEIVE_ROUTY_KEY = "DC.*.transform";
	private static final String RECEIVE_QUEUE = "DC.transform";  

	
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		LOG.info("init DefaultMQPushConsumer");
		this.collector = collector;
		this.id = context.getThisComponentId() + ":" + context.getThisTaskId();
		System.out.println(" [bug] id........................."+id);  
		LOG.info("init channel");
    	chan = RocketMQUtil.getChannel();
		LOG.info("init Consumer ");
	    try {
	    	// 声明转发器 
//	    	chan.exchangeDeclare(EXCHANGE_NAME, "topic");  
	    	// 接收所有与kernel相关的消息  
	        chan.queueBind(RECEIVE_QUEUE, EXCHANGE_NAME, RECEIVE_ROUTY_KEY); 
	    	// 声明消费者  
	    	consumer = new QueueingConsumer(chan);
			chan.basicConsume(RECEIVE_QUEUE, true, consumer);//如果是false 需要接受方发送ack回执,删除消息
//            chan.basicQos(1);  //这样RabbitMQ就会使得每个Consumer在同一个时间点最多处理一个Message。换句话说，在接收到该Consumer的ack前，他它不会将新的Message分发给它。 

//			System.out.println(" [x] consumer........................."+consumer+QUEUE_NAME);  
		} catch (IOException e) {
			e.printStackTrace();
		}
        System.out.println(" [x] start.........................");  
		LOG.info("Successfully init " + id);
	}

	@Override
	public void nextTuple() {
		String message = null;
//		String routingKey = null;
		// 等待队列推送消息
		try {
			delivery = consumer.nextDelivery();
			while (delivery != null) {
				message = new String(delivery.getBody(), "UTF-8");
//				routingKey = delivery.getEnvelope().getRoutingKey();
//				System.out.println(" [x] Received '" + routingKey + "':'" + message + "'");
				// 反馈给服务器表示收到信息 与channel.BasicConsume("task_queue", false, null,consumer);对应
				// chan.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				collector.emit(new Values(message));
//				System.out.println(" [x] start send !'");
				Thread.sleep(6000);
			}
		} catch (ShutdownSignalException | ConsumerCancelledException | InterruptedException | IOException e2) {
			e2.printStackTrace();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("message"));
	}

	
}