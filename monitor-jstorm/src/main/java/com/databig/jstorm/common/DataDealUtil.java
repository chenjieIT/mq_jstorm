package com.databig.jstorm.common;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.databig.jstorm.common.log.LogConfigUtils;

/**   
* Copyright (c) 2016 Founder Ltd. All Rights Reserved.
* Company:昆明能讯科技
* @Title: DataDealUtil.java 
* @Package com.databig.jstorm.common 
* @Description: TODO(数据处理) 
* @author enersun_lhb  
* @date 2016年8月17日 下午2:06:59 
* @version V1.0   
*/
public class DataDealUtil {

	/*
	 * 接受到的消息内容，json中字段定义
	 * 发送的消息内容，json字段定义
	 */
	private static String TERMINAL_CODE = "terminalCode";
	private static String TOPIC_ID = "topicId";
	private static String ATTRIBUTE = "attribute";
	private static String DATATIME = "datatime";
	private static String THEME_ID = "theme_id";
	private static String THEMEID = "themeId";
	private static String OWNER_TYPE = "owner_type";
	private static OracleJdbcUtil oracleJdbcUtil;
	
	@SuppressWarnings("rawtypes")
	public static String transferTerminalToThemeforHbase(String message){
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
			String termialCode = (String) value.get(TERMINAL_CODE);
			String type =  (String) value.get(TOPIC_ID);
			String ower_type= (String) value.get(OWNER_TYPE);
			attrJsonObject =  (JSONObject) value.get(ATTRIBUTE);
			attrJsonStr=attrJsonObject.toString();
			String datatime = (String) value.get(DATATIME);
			childJsonMap =  (Map) JSON.parse(attrJsonStr); 
			String themeId = selectThemeIdByTermialCodeAndType(ower_type,termialCode,type);
			for (Map.Entry<String, String> entry : childJsonMap.entrySet()) {
				rowKey = themeId + entry.getKey() + datatime;
				sendVal.put(rowKey, entry.getValue());// 改变zzmm的值
			}
			
		}
		result=JSON.toJSONString(sendVal);
		return result;
	}

	@SuppressWarnings("rawtypes")
	public static String transferTerminalToTheme(String message){
		String result = null;
		List<Map> meteorologicalsOld = JSON.parseArray(message, Map.class); 
		
		List<Map> meteorologicalsNew = new ArrayList<Map>();
		Map<String, String> childJsonMap = null; 

		Map<String,Object> value = new HashMap<String,Object>(); 
		Map<String,String> sendVal = new HashMap<String,String>();
		JSONObject attrJsonObject;
		String attrJsonStr;
		String rowKey =null;
		for (int i = 0; i < meteorologicalsOld.size(); i++) {
			value = meteorologicalsOld.get(i);
			String termialCode = (String) value.get(TERMINAL_CODE);
			String type =  (String) value.get(TOPIC_ID);
			attrJsonObject =  (JSONObject) value.get(ATTRIBUTE);
			String datatime = (String) value.get("DATATIME");
			String ower_type= (String) value.get(OWNER_TYPE);
			String themeId = selectThemeIdByTermialCodeAndType(ower_type,termialCode, type);
			value.remove(TERMINAL_CODE);
			value.put(THEMEID, themeId);
			meteorologicalsNew.add(value);
		}
		result=JSON.toJSONString(meteorologicalsNew);
		return result;
	}
	
	/**
	 * 
	* @Title: selectThemeIdByTermialCodeAndType 
	* @Description: TODO(终端转主题) 
	* @param @param termialCode
	* @param @param type
	* @param @return    设定文件 
	* @return String    返回类型 
	* @throws
	 */
	public static String selectThemeIdByTermialCodeAndType(String ower_type ,String termialCode, String type) {
		String sql = "select p.theme_id from theme_terminal_map p where p.terminal_id in "
				+ "(select id from terminal_device_info where owner_type=? and ext_sys_tb_id=?"
				+ ") and topic_id=?";
		 ResultSet rs= oracleJdbcUtil.query(sql,ower_type, termialCode,  type);
		 String themeId = null;
		 try {
			if(rs.next()){
				 themeId = rs.getString(THEME_ID);
			 }
		} catch (SQLException e) {
			LogConfigUtils.logError("[x]get themeId is null ,occured "+e.getMessage(), e.getCause());
		}finally{  
            close();  
        }  
		return themeId;
	}
	
	public static void getDB(){
		oracleJdbcUtil= new OracleJdbcUtil();
	}
	
	public static void close(){
		oracleJdbcUtil.close();
	}
}
