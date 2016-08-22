package com.databig.jstorm.common;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Copyright (c) 2016 Founder Ltd. All Rights Reserved. Company:昆明能讯科技
 * 
 * @Title: DBConnection.java
 * @Package com.databig.jstorm.common
 * @Description: TODO(用一句话描述该文件做什么)
 * @author enersun_lhb
 * @date 2016年8月5日 上午11:12:36
 * @version V1.0 使用类DBConnection 调用连接池，并且接管transaction控制，进行统一的commit 和 rollback
 *          完成释放链接池的操作.
 */
public class DBConnection {
	private Connection conn = null;
	private DBConnectionManager dcm = null;
	private String connName = null;

	// 初始化
	void init() {
		dcm = DBConnectionManager.getInstance();
		conn = dcm.getConnection(connName);
		while (conn == null) {
			conn = dcm.getConnection(connName, 10);
		}
	}

	/**
	 * 构造数据库的连接和访问类
	 * 
	 * @param connName
	 *            连接池名称
	 */
	public DBConnection(String connName) throws Exception {
		this.connName = connName;
		init();
	}

	/**
	 * 返回预设状态
	 */
	public PreparedStatement getPreparedStatement(String sql) throws SQLException {
		return conn.prepareStatement(sql);
	}

	/**
	 * 返回状态
	 */
	public Statement getStatement() throws SQLException {
		return conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
	}

	/**
	 * 开始数据库事务
	 * 
	 * @throws SQLException
	 *             数据库连接对象异常
	 */
	public void beginTransaction() throws SQLException {
		// 禁用连接的自动提交模式
		this.conn.setAutoCommit(false);
	}

	/**
	 * 提交数据库事务
	 * 
	 * @throws SQLException
	 *             数据库连接对象异常
	 */
	public void commitTransaction() throws SQLException {
		// 手动提交事务
		this.conn.commit();
		// 启用连接的自动提交模式
		this.conn.setAutoCommit(true);
	}

	/**
	 * 回滚数据库事务
	 * 
	 * @throws SQLException
	 *             数据库连接对象异常
	 */
	public void rollbackTransaction() throws SQLException {
		// 取消并回滚事务
		this.conn.rollback();
		// 启用连接的自动提交模式
		this.conn.setAutoCommit(true);
	}

	/**
	 * 关闭连接
	 */
	public void close() throws Exception {
		if (conn != null) {
			dcm.freeConnection(connName, conn);
		}
	}
}