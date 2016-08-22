package com.databig.jstorm.common.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**   
* Copyright (c) 2016 Founder Ltd. All Rights Reserved.
* Company:昆明能讯科技
* @Title: LogConfigUtils.java 
* @Package com.databig.jstorm.common.log 
* @Description: TODO(日志格式) 
* @author enersun_lhb  
* @date 2016年8月17日 下午5:25:46 
* @version V1.0   
*/
public class LogConfigUtils {
	    /** 
	     * 错误输入日志 
	     */  
	    public static final Logger log = LoggerFactory.getLogger(LogConfigUtils.class);  
	  
	    /** 
	     * 记录一直 info信息 
	     *  
	     * @param message 
	     */  
	    public static void logInfo(String message) {  
	        StringBuilder s = new StringBuilder();  
	        s.append((message));  
	        log.info(s.toString());  
	    }  
	  
	    public static void logInfo(String message, Throwable e) {  
	        StringBuilder s = new StringBuilder();  
	        s.append(("exception : -->>"));  
	        s.append((message));  
	        log.info(s.toString(), e);  
	    }  
	  
	    public static void logWarn(String message) {  
	        StringBuilder s = new StringBuilder();  
	        s.append((message));  
	  
	        log.warn(s.toString());  
	    }  
	  
	    public static void logWarn(String message, Throwable e) {  
	        StringBuilder s = new StringBuilder();  
	        s.append(("exception : -->>"));  
	        s.append((message));  
	        log.warn(s.toString(), e);  
	    }  
	  
	    public static void logDebug(String message) {  
	        StringBuilder s = new StringBuilder();  
	        s.append((message));  
	        log.debug(s.toString());  
	    }  
	  
	    public static void logDebug(String message, Throwable e) {  
	        StringBuilder s = new StringBuilder();  
	        s.append(("exception : -->>"));  
	        s.append((message));  
	        log.debug(s.toString(), e);  
	    }  
	  
	    public static void logError(String message) {  
	        StringBuilder s = new StringBuilder();  
	        s.append(message);  
	        log.error(s.toString());  
	    }  
	  
	    /** 
	     * 记录日志错误信息 
	     *  
	     * @param message 
	     * @param e 
	     */  
	    public static void logError(String message, Throwable e) {  
	        StringBuilder s = new StringBuilder();  
	        s.append(("exception : -->>"));  
	        s.append((message));  
	        log.error(s.toString(), e);  
	    }  
}
