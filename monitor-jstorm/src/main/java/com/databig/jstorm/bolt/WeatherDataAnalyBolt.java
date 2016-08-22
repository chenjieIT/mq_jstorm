package com.databig.jstorm.bolt;

import java.io.IOException;
import java.util.Map;


import com.databig.jstorm.common.DataDealUtil;
import com.databig.jstorm.common.RocketMQUtil;
import com.databig.jstorm.common.constant.CommonConstant;
import com.databig.jstorm.common.log.LogConfigUtils;
import com.rabbitmq.client.Channel;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**   
* Copyright (c) 2016 Founder Ltd. All Rights Reserved.
* Company:昆明能讯科技
* @Title: MonitorAnalyBolt.java 
* @Package com.databig.jstorm.bolt 
* @Description: TODO(监测数据分析Bolt) 
* @author enersun_lhb  
* @date 2016年7月27日 下午4:01:35 
* @version V1.0   
*/
public class WeatherDataAnalyBolt extends BaseRichBolt  {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8464121589744039001L;
	
	private OutputCollector collector;	
	private static Channel channel = null;
	private static final String[] SEND_ROUTY_KEY1= new String[]{"DC.weather.show","DC.weather.store"};
	private static final String[] SEND_ROUTY_KEY2= new String[]{"DC.dmis.show","DC.dmis.store"};


	
	public void execute(Tuple tuple) {
		String message = tuple.getStringByField("message");
		String dataId = tuple.getStringByField("dataId");
		if(dataId.indexOf(CommonConstant.MonitorType.WEATHER)>=0){
			sendToRabbitMQ(message,SEND_ROUTY_KEY1);
		}
		if(dataId.indexOf(CommonConstant.MonitorType.DMIS)>=0){
			sendToRabbitMQ(message,SEND_ROUTY_KEY2);
		}
//		collector.emit(new Values(newMsg));
        // 发送ack信息告知spout 完成处理的消息 ，如果下面的hbase的注释代码打开了，则必须等到插入hbase完毕后才能发送ack信息，这段代码需要删除
        collector.ack(tuple);	
    	LogConfigUtils.logInfo("receive from spout reply!");

	}
		
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("transformMessage"));
	}


   public void sendToRabbitMQ(String message,String [] routy_key){
	   String newMsg ="";
	   if(message!= null && !"".equals(message)){
			for(String routing_key : routy_key){
		      try {
		    	  
		    	  if(routing_key.contains(CommonConstant.MonitorType.STORE)){
		    		  newMsg = DataDealUtil.transferTerminalToThemeforHbase(message);
		    	  }
		    	  if(routing_key.contains(CommonConstant.MonitorType.SHOW)){
		    		  newMsg = DataDealUtil.transferTerminalToTheme(message);
		    	  }
	    		  channel.basicPublish(CommonConstant.RabbitMqConstant.EXCHANGE_NAME, routing_key, null, newMsg.getBytes());
//		    	  System.out.println(" [x] Sent routingKey = " + routing_key + 
//		    			  " ,msg = " + newMsg + ".");
		      } catch (IOException  e) {
		    	  LogConfigUtils.logError("[x]Sent routingKey = " + routing_key+e.getMessage(),e.getCause());
		      }   
		     }
			newMsg=null;
		}
   }
	

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;	
		LogConfigUtils.logInfo("init dbmanager ");
		DataDealUtil.getDB();
		LogConfigUtils.logInfo("init channel");
    	channel = RocketMQUtil.getChannel();
		try {
			//声明转发器  
//			channel.exchangeDeclare(EXCHANGE_NAME, "topic",true);
			//持久化  
			channel.queueDeclare(CommonConstant.RabbitMqConstant.SEND_QUEUE, true, false, false, null);  
			channel.basicQos(1);  
			//将消息队列绑定到Exchange  
//			channel.queueBind(SEND_QUEUE, EXCHANGE_NAME, SEND_ROUTY_KEY);  
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void cleanup(){
		
	}
}
