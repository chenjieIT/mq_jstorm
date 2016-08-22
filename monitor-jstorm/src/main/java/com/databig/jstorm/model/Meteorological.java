package com.databig.jstorm.model;

import java.io.Serializable;
import java.util.Arrays;

/**   
* Copyright (c) 2016 Founder Ltd. All Rights Reserved.
* Company:昆明能讯科技
* @Title: Meteorological.java 
* @Package com.databig.jstorm.model 
* @Description: TODO(气象数据) 
* @author enersun_lhb  
* @date 2016年7月27日 下午4:35:22 
* @version V1.0   
*/
public class Meteorological implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7031687665437850055L;
	
	private String  terminalCode;
	private String topicId;
	private String datatime;
	private String[] attribute;
	
	


	public Meteorological(String terminalCode, String topicId, String datatime, String[] attribute) {
		super();
		this.terminalCode = terminalCode;
		this.topicId = topicId;
		this.datatime = datatime;
		this.attribute = attribute;
	}


	public Meteorological() {
		super();
	}


	public String getTerminalCode() {
		return terminalCode;
	}


	public void setTerminalCode(String terminalCode) {
		this.terminalCode = terminalCode;
	}


	public String getTopicId() {
		return topicId;
	}


	public void setTopicId(String topicId) {
		this.topicId = topicId;
	}


	public String getDatatime() {
		return datatime;
	}


	public void setDatatime(String datatime) {
		this.datatime = datatime;
	}


	public String[] getAttribute() {
		return attribute;
	}


	public void setAttribute(String[] attribute) {
		this.attribute = attribute;
	}


	@Override
	public String toString() {
		return "Meteorological [terminalCode=" + terminalCode + ", topicId=" + topicId + ", datatime=" + datatime
				+ ", attribute=" + Arrays.toString(attribute) + "]";
	}



	
	
}
