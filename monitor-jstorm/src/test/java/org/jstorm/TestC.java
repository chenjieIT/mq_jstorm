package org.jstorm;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.databig.jstorm.common.OracleJdbcUtil;

public class TestC {

	public static void main(String[] args) {
		int i,j,m=1;
//		while(true){
//			System.out.println(++n);
//			if (n ==10) break;
//		}
		for(i=1;i<3;i++){
			for(j=3;j>0;j--){
//				System.out.println("i="+i+" ;j="+j);
				if(i*j>3)break;
				m+=i*j;
				String themeId = selectThemeIdByTermialCodeAndType(m+"",j+"");
//				System.out.println("m:"+m+";i:"+i+" ;j:"+j);
			}
		}
		System.out.println("最后m="+m);
	}
	
	public static String selectThemeIdByTermialCodeAndType(String termialCode, String type) {
		OracleJdbcUtil oracleJdbcUtil = new OracleJdbcUtil();
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
		}
		return themeId;
	}


}
