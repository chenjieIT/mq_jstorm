package com.databig.jstorm.common.constant;

/**   
* Copyright (c) 2016 Founder Ltd. All Rights Reserved.
* Company:昆明能讯科技
* @Title: CommonConstant.java 
* @Description: TODO(common模块的一些常量或者不在库中字典定义) 
* @author enersun_lhb   
* @date 2016年6月13日 上午11:51:31 
* @version V1.0   
*/

public class CommonConstant {

	// 模块RabbitMqConstant
	public  class RabbitMqConstant {
		
		public RabbitMqConstant(){}
		
		public static final String EXCHANGE_NAME = "DC.Exchange";  
		public static final String RECEIVE_ROUTY_KEY = "DC.*.transform";
		public static final String RECEIVE_QUEUE = "DC.transform";  
		public static final String SEND_QUEUE = "DC.show";
		public static final String MONITOR = "DC.show";
	}

	
		//监测列表类型
		public final class MonitorType{
			public static final String  WEATHER= "weather";//气象数据
			public static final String DMIS = "dmis";//调度
			public static final String  STORE= "store";//存储
			public static final String SHOW = "show";//显示
		}

	}

