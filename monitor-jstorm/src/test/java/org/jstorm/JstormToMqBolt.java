package org.jstorm;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.databig.jstorm.common.OracleJdbcUtil;
import com.databig.jstorm.common.RocketMQUtil;
import com.rabbitmq.client.Channel;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class JstormToMqBolt extends BaseRichBolt {

	private  static Logger LOG = LoggerFactory.getLogger(JstormToMqBolt.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;	
	private OracleJdbcUtil oracleJdbcUtil;
	private static Channel channel = null;
	private static final String EXCHANGE_NAME = "DC.Exchange";  
	private static final String[] SEND_ROUTY_KEY = new String[]{"DC.weather.show","DC.weather.store"};
	private static final String SEND_QUEUE = "DC.show";
	
	/*
	 * execute() => most important method in the bolt is execute(Tuple input),
	 * which is called once per tuple received the bolt may emit several tuples
	 * for each tuple received
	 */
	public void execute(Tuple tuple) {
		String message = tuple.getStringByField("message");
		String newMsg ="";
		if(message!= null && !"".equals(message)){
//			System.out.println("Found a Hello World ! My Count is now :" + message );
			for(String routing_key : SEND_ROUTY_KEY){
		      try {
		    	  if(!"DC.weather.show".equalsIgnoreCase(routing_key)){
		    		  newMsg = transferTerminalToThemeforHbase(message);
		    	  }else{
		    		  newMsg = transferTerminalToTheme(message);
		    	  }
	    		  channel.basicPublish(EXCHANGE_NAME, routing_key, null, newMsg.getBytes());
		    	  System.out.println(" [x] Sent routingKey = " + routing_key + 
		    			  " ,msg = " + newMsg + ".");
		      } catch (IOException  e) {
		    	  LOG.error("[x]Sent routingKey = " + routing_key+", IOException:"+e);
		      }   
		     }
			newMsg=null;
		}
//		collector.emit(new Values(newMsg));
        // 发送ack信息告知spout 完成处理的消息 ，如果下面的hbase的注释代码打开了，则必须等到插入hbase完毕后才能发送ack信息，这段代码需要删除
        collector.ack(tuple);	
		LOG.info("receive from spout reply!");

	}

	/*
	 * declareOutputFields => This bolt emits nothing hence no body for
	 * declareOutputFields()
	 */
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("transformMessage"));
	}

	public String transferTerminalToThemeforHbase(String message){
		String result = null;
		List<Map> meteorologicalsOld = JSON.parseArray(message, Map.class); 
		
		List<Map> childJsonAttribute = null; 
		Map<String, String> childJsonMap = null; 

		Map<String,Object> value = new HashMap<String,Object>(); ;
		Map<String,String> sendVal = new HashMap<String,String>();
		JSONObject attrJsonObject;
		String attrJsonStr;
		String rowKey =null;
		for (int i = 0; i < meteorologicalsOld.size(); i++) {
			value = meteorologicalsOld.get(i);
			String termialCode = (String) value.get("terminalCode");
			String type =  (String) value.get("topicId");
			attrJsonObject =  (JSONObject) value.get("attribute");
			attrJsonStr=attrJsonObject.toString();
			String datatime = (String) value.get("datatime");
			childJsonMap =  (Map) JSON.parse(attrJsonStr); 
			String themeId = selectThemeIdByTermialCodeAndType(termialCode,type);
			for (Map.Entry<String, String> entry : childJsonMap.entrySet()) {
//				System.out.println("key= " + entry.getKey() + " and value= " + entry.getValue());
				rowKey = themeId + entry.getKey() + datatime;
				sendVal.put(rowKey, entry.getValue());// 改变zzmm的值
//				System.out.println("themeId " + rowKey);
			}
			
		}
		result=JSON.toJSONString(sendVal);
//		System.out.println(result);
		return result;
	}

	public String transferTerminalToTheme(String message){
		String result = null;
		List<Map> meteorologicalsOld = JSON.parseArray(message, Map.class); 
		
		List<Map> meteorologicalsNew = new ArrayList<Map>();
		Map<String, String> childJsonMap = null; 

		Map<String,Object> value = new HashMap<String,Object>(); 
		Map<String,String> sendVal = new HashMap<String,String>();
		JSONObject attrJsonObject;
		String attrJsonStr;
		String rowKey =null;
//		List collection = new ArrayList<KeyValueMap>();
		for (int i = 0; i < meteorologicalsOld.size(); i++) {
			value = meteorologicalsOld.get(i);
			String termialCode = (String) value.get("terminalCode");
			String type =  (String) value.get("topicId");
			attrJsonObject =  (JSONObject) value.get("attribute");
//			attrJsonStr=attrJsonObject.toString();
			String datatime = (String) value.get("datatime");
//			childJsonMap =  (Map) JSON.parse(attrJsonStr); 
			String themeId = selectThemeIdByTermialCodeAndType(termialCode, type);
			value.remove("terminalCode");
			value.put("themeId", themeId);
			meteorologicalsNew.add(value);
		}
		result=JSON.toJSONString(meteorologicalsNew);
//		System.out.println(result);
		return result;
	}
	
	public String selectThemeIdByTermialCodeAndType(String termialCode, String type) {
		String sql = "select p.theme_id from theme_terminal_map p where p.terminal_id in "
				+ "(select id from terminal_device_info where owner_type='4' and ext_sys_tb_id=?"
				+ ") and topic_id=?";
		 ResultSet rs= oracleJdbcUtil.query(sql, termialCode,  type);
		 String themeId = null;
		 try {
			if(rs.next()){
				 themeId = rs.getString("theme_id");
			 }
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{  
            close();  
        }  
		return themeId;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;	
		LOG.info("init dbmanager ");
		getDB();
		LOG.info("init channel");
    	channel = RocketMQUtil.getChannel();
		try {
			//声明转发器  
//			channel.exchangeDeclare(EXCHANGE_NAME, "topic",true);
			//持久化  
			channel.queueDeclare(SEND_QUEUE, true, false, false, null);  
			channel.basicQos(1);  
			//将消息队列绑定到Exchange  
//			channel.queueBind(SEND_QUEUE, EXCHANGE_NAME, SEND_ROUTY_KEY);  
		} catch (IOException e) {
			e.printStackTrace();
		}  
	}

	public void getDB(){
		oracleJdbcUtil= new OracleJdbcUtil();
	}
	
	public void close(){
		oracleJdbcUtil.close();
	}
}

 class KeyValueMap {

    private Map<String, Object> hashMap;

    public Map<String, Object> getHashMap() {
        return hashMap;
    }

    public void setHashMap(Map<String, Object> hashMap) {
        this.hashMap = hashMap;
    }
}
