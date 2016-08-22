package com.databig.jstorm.common;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import com.databig.jstorm.common.log.LogConfigUtils;



/**   
* Copyright (c) 2016 Founder Ltd. All Rights Reserved.
* Company:昆明能讯科技
* @Title: JstormUtil.java 
* @Package com.databig.jstorm.common 
* @Description: TODO(jstorm 数据处理工具) 
* @author enersun_lhb  
* @date 2016年8月2日 下午1:25:06 
* @version V1.0   
*/
public class OracleJdbcUtil {
	private static String oracleDb_Driver = null;
    private static String oracleDb_Url = null;
	private static String oracleDb_UserName = null;
    private static String oracleDb_Password = null;
    private static PreparedStatement sta = null;  
    private static ResultSet rs = null;  
    private static Connection conn = null;  

    /** 
     * 加载驱动程序 
     */  
    static {  
        try {  
        	//读取db.properties文件中的数据库连接信息
        	Properties prop = new Properties();
        	InputStream in = OracleJdbcUtil.class.getResourceAsStream("/db.properties");
        	
        	prop.load(in);
        	 
        	 //获取数据库连接驱动
        	 oracleDb_Driver = prop.getProperty("oracleDb_Driver");
        	 //获取数据库连接URL地址
        	 oracleDb_Url = prop.getProperty("oracleDb_Url");
        	 //获取数据库连接用户名
        	 oracleDb_UserName = prop.getProperty("oracleDb_UserName");
        	 //获取数据库连接密码
        	 oracleDb_Password = prop.getProperty("oracleDb_Password");
        	 //加载数据库驱动
        	 Class.forName(oracleDb_Driver); 
        	 in.close();
        } catch (ClassNotFoundException | IOException e) {  
        	LogConfigUtils.logError(e.getMessage(), e.getCause());
        }  
    }  

    public OracleJdbcUtil() 
    { //构造方法中生成连结  
    //无论是从DataSource还是直接从DriverManager中取得连结.  
    //先初始化环境,然后取得连结,本例作为初级应用,从  
    //DriverManager中取得连结,因为是封装类,所以要把异常抛  
    //给调用它的程序处理而不要用try{}catch(){}块自选处理了.  
    //因为要给业务方法的类继承,而又不能给调用都访问,所以  
    //conn声明为protected  
    	try {
			conn =  DriverManager.getConnection(oracleDb_Url, oracleDb_UserName,oracleDb_Password);
		} catch (SQLException e) {
			LogConfigUtils.logError(e.getMessage(), e.getCause());
		}  
    }
    /** 
     * @return 连接对象 
     */  
    protected  Connection makeConnection() {  
        try {  
            conn = DriverManager.getConnection(oracleDb_Url, oracleDb_UserName,oracleDb_Password);
        } catch (SQLException e) {  
        	LogConfigUtils.logError(e.getMessage(), e.getCause());
        }  
        return conn;
    }  

    /** 
     * @param sql 
     *            sql语句  增加，删除，修改 
     * @param obj 
     *            参数 
     * @return 
     * @throws SQLException 
     */  
    public  int update(String sql, Object... obj)  {  
        int count = 0;  
        try {
			if(conn ==null ||conn.isClosed()){
				conn = makeConnection();  
			}
		} catch (SQLException e1) {
			LogConfigUtils.logError(e1.getMessage(), e1.getCause());
		}
        try {  
            sta = conn.prepareStatement(sql);  
            if (obj != null) {  
                for (int i = 0; i < obj.length; i++) {  
                    sta.setObject(i + 1, obj[i]);  
                }  
            }  
            count = sta.executeUpdate();  
        } catch (SQLException e) {  
        	LogConfigUtils.logError(e.getMessage(), e.getCause()); 
        } finally{  
          
            close();  
        }  
        return count;  
    }  

    /** 
     * @param sql sql语句 
     * @param obj 参数 
     * @return 数据集合 
     */  
    public  ResultSet query(String sql,Object...obj){  
    	 try {
 			if(conn ==null ||conn.isClosed()){
 				conn = makeConnection();  
 			}
 		} catch (SQLException e1) {
 			LogConfigUtils.logError(e1.getMessage(), e1.getCause());
 		}
        try {  
            sta=conn.prepareStatement(sql);  
            if(obj!=null){  
                for(int i=0;i<obj.length;i++){  
                    sta.setObject(i+1, obj[i]);  
                }  
            }  
            rs=sta.executeQuery();  
        } catch (SQLException e) {  
        	LogConfigUtils.logError(e.getMessage(), e.getCause());
        }  
        return rs;  
    }  
      
    /** 
     * 关闭资源 
     */  
    public  void close() {  
        try {  
            if (rs != null) {  
                rs.close();  
            }  
        } catch (SQLException e) {  
        	LogConfigUtils.logError(e.getMessage(), e.getCause());
        } finally {  
            try {  
                if (sta != null) {  
                    sta.close();  
                }  
            } catch (SQLException e2) {  
            	LogConfigUtils.logError(e2.getMessage(), e2.getCause());
            } finally {  
                if (conn != null) {  
                    try {  
                        conn.close();  
                    } catch (SQLException e) {  
                    	LogConfigUtils.logError(e.getMessage(), e.getCause());
                    }  
                }  
            }  
        }  
    }  

}  