package com.databig.jstorm.scheme;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * 自定义MQ消息的Scheme
 * @author enersun_lhb
 *
 */

public class RabbitMqScheme implements Scheme {

	/**
	 * 继承并实现 backtype.storm.spout.Scheme来反序列化RabbitMQ的消息
	 */
	private static final long serialVersionUID = 1L;

	/**
	   * 把MQ中读取的消息反序列化
	   */
	public List<Object> deserialize(final byte[] ser) {  
	    ////直接反序列化为string
	    return new Values(deserializeString(ser)); 
	   
	  }  

	public static String deserializeString(byte[] string) {  
        try {  
            return new String(string, "UTF-8");  
        } catch (UnsupportedEncodingException e) {  
            throw new RuntimeException(e);  
        }  
    }  
	 /**
	   * 定义spout输出的Fileds
	   */
	public Fields getOutputFields() {  
		//返回String，需要与上述返回的List列表一一对应
		 return new Fields("Meassage");    
	  }  

}
