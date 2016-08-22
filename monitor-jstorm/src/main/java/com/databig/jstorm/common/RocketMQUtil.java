package com.databig.jstorm.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**   
* Copyright (c) 2016 Founder Ltd. All Rights Reserved.
* Company:昆明能讯科技
* @Title: RocketMQUtil.java 
* @Package com.databig.jstorm 
* @Description: TODO(RocketMQ连接工具类) 
* @author enersun_lhb  
* @date 2016年8月1日 下午1:14:09 
* @version V1.0   
*/
public class RocketMQUtil {

	private static final Logger log = LoggerFactory.getLogger(RocketMQUtil.class);
	private static Channel channel = null;
	private static final String EXCHANGE_NAME = "DC.Exchange";  
	private static final String SEND_ROUTY_KEY = "DC.weather.show";
	private static final String RECEIVE_ROUTY_KEY = "DC.*.transform";
	private static final String RECEIVE_QUEUE = "DC.transform";  
	private static final String SEND_QUEUE = "DC.weather.show"; 
	public static Channel  getChannel(){
		channel=RocketMqConnectionFactory.getChannel();//获取MQ资源
		return channel;
		
	}
	/**
	 *
	* @Title: mqSendMassages 
	* @Description: TODO(topic（主题）模式 MQ生产者) 
	* @param @param msg    设定文件 
	* @return void    返回类型 
	* @throws
	 */
    public static void mqSendMassages(String message, Channel chann){

    	/*创建消息队列，并且发送消息*/
    	try {
    		  //声明转发器  
    			chann.exchangeDeclare(EXCHANGE_NAME, "topic",true);  
    			//随机生成队列
//    	        String queueName = channel.queueDeclare().getQueue();
    	        System.out.println(" [*] Waiting for send !"+SEND_QUEUE);
    	        //持久化  
    	        chann.queueDeclare(SEND_QUEUE, true, false, false, null);  
    	        chann.basicQos(1);  
    	        //将消息队列绑定到Exchange  
    	        chann.queueBind(SEND_QUEUE, EXCHANGE_NAME, SEND_ROUTY_KEY);  
    	       //需要持久化Message，即在Publish的时候指定一个properties，  
    	  
    	        chann.basicPublish(EXCHANGE_NAME, SEND_ROUTY_KEY, null, message.getBytes());   
    	    
    	      System.out.println(" [x] Sent '" + SEND_ROUTY_KEY + "':'" + message.getBytes() + "'");
    	      RocketMqDSClose.close(chann);
    	} catch (IOException e) {
    		log.error("error IOException ：" + e);
    	}

	}
    
    public static void mqReceiveMassages(String routkey,Channel chann){
    	/*接受消息*/
    	try {
    		log.info("start recevie !" );
    	   //指定一个topic类型的exchang
          // 声明转发器    
//             channel.exchangeDeclare(EXCHANGE_NAME, "topic",true);   
             //持久化  
//             String queueName = channel.queueDeclare().getQueue();
//             System.out.println(" [*] Waiting for recevice!"+queueName);
//             chann.queueDeclare(routkey, true, false, false, null);  

             chann.basicQos(1);  //这样RabbitMQ就会使得每个Consumer在同一个时间点最多处理一个Message。换句话说，在接收到该Consumer的ack前，他它不会将新的Message分发给它。 
             //将消息队列绑定到Exchange  
             channel.queueBind(RECEIVE_QUEUE, EXCHANGE_NAME, RECEIVE_ROUTY_KEY);  
             channel.queueDeclare(RECEIVE_QUEUE, false, false, false, null);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C!!! ["+RECEIVE_QUEUE+"]");
              // 声明消费者  
    	    QueueingConsumer consumer = new QueueingConsumer(chann);
    	    chann.basicConsume(RECEIVE_QUEUE, true, consumer);//如果是false 需要接受方发送ack回执,删除消息
            
    	    while (true) {
    	    	// 等待队列推送消息  
    	        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
    	        String message = new String(delivery.getBody(), "UTF-8");
    	        String routingKey = delivery.getEnvelope().getRoutingKey();
//    	        Thread.sleep(1000);
    	        System.out.println(" [x] Received '" + routingKey + "':'" + message + "'");  
    	        // 反馈给服务器表示收到信息   与channel.BasicConsume("task_queue", false, null, consumer);对应
//    	        chann.basicAck(delivery.getEnvelope().getDeliveryTag(), false); 
    	      }
    	} catch (IOException e) {
    		log.error("error IOException ：" + e);
    	} catch (ShutdownSignalException e) {
    		log.error("error ShutdownSignalException ：" + e);
		} catch (ConsumerCancelledException e) {
			log.error("error ConsumerCancelledException ：" + e);
		} catch (InterruptedException e) {
			log.error("error InterruptedException ：" + e);
		}

	}

   
}

class RocketMqDataSourcHolder {  
	/*使用工厂类建立Connection和Channel，并且设置参数*/
	 private ConnectionFactory factory = new ConnectionFactory();  
	 private Connection connection = null;
     public static String RABBITMQ_NODE_IP_ADDRESS = null;
     public static String RABBITMQ_NODE_PORT = null;
	  /** 
	     * 加载驱动程序 
	     */  
	    static {  
	        try {  
	        	//读取db.properties文件中的数据库连接信息
	        	Properties prop = new Properties();
	        	InputStream in = RocketMqDataSourcHolder.class.getResourceAsStream("/rabbitmq.properties");
	        	prop.load(in);
	        	 
	        	 //#IP地址，空串bind所有地址，指定地址bind指定网络接口
	        	RABBITMQ_NODE_IP_ADDRESS = prop.getProperty("RABBITMQ_NODE_IP_ADDRESS");
	        	 //#TCP端口号
	        	RABBITMQ_NODE_PORT = prop.getProperty("RABBITMQ_NODE_PORT");
	        
	        	 in.close();
	        } catch (IOException e) {  
	            e.printStackTrace();  
	        }  
	    }  

	    private RocketMqDataSourcHolder() {  
	    	 factory.setHost("10.111.58.120");//MQ的IP
	    	 factory.setPort(5672);//MQ端口
	    	 factory.setUsername("mqadmin");//MQ用户名
	    	 factory.setPassword("mqadmin");//MQ密码
	    	 try {
				connection = factory.newConnection();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (TimeoutException e) {
				e.printStackTrace();
			}
	    }  
	    private static class SingletonHolder{  
	        private static RocketMqDataSourcHolder instance = new RocketMqDataSourcHolder();  
	    }  
	    public static RocketMqDataSourcHolder getInstance(){  
	        return SingletonHolder.instance;  
	    }
		public Channel createChannel() throws IOException {
			return connection.createChannel();
		}  
}

class RocketMqConnectionFactory {  
	 
    public static Channel getChannel() {  
    	Channel channel = null;  
    	try {
			channel = RocketMqDataSourcHolder.getInstance().createChannel();
		} catch (IOException e) {
			e.printStackTrace();
		}
         
  
        return channel;  
    }  
} 

class RocketMqDSClose {  
    public static void close(Channel channel) {  
        if (null != channel) {  
            	try {
					channel.close();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (TimeoutException e) {
					e.printStackTrace();
				}  
        }  
    }  
  
    public static void close(Connection connection) {  
        if (null != connection) {  
        	try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }  
    }  
} 