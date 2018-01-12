package com.lsy.sparkproject.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

import com.lsy.sparkproject.conf.ConfigurationManager;
import com.lsy.sparkproject.constant.Constants;

/**
 * JDBC辅助组件
 * 
 * @author work
 * 
 */
public class JDBCHelper {
	// 1.在静态代码块中，直接加载数据库驱动
	static {
		try {
			String driver = ConfigurationManager
					.getProperty(Constants.JDBC_DRIVER);
			Class.forName(driver);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 2.实现JDBCHeler的单例化，保证只有一个数据库连接池
	private static JDBCHelper instance = null;

	private LinkedList<Connection> datasource = new LinkedList<Connection>();

	private JDBCHelper() {
		// 3.创建数据库连接池
		int datasourceSize = ConfigurationManager
				.getInterger(Constants.JDBC_DATASOURCE_SIZE);
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		
		String url = null;
		String user = null;
		String password = null;
		if (local) {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER);
			password = ConfigurationManager
					.getProperty(Constants.JDBC_PASSWORD);
		}else{
			url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
			password = ConfigurationManager
					.getProperty(Constants.JDBC_PASSWORD_PROD);
		}
		for (int i = 0; i < datasourceSize; i++) {
			try {
				Connection conn = DriverManager.getConnection(url, user,
						password);
				datasource.push(conn);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static JDBCHelper getInstance() {
		if (instance == null) {
			synchronized (JDBCHelper.class) {
				if (instance == null) {
					instance = new JDBCHelper();
				}

			}
		}
		return instance;
	}

	// 4.提供获取数据库连接的方法
	public synchronized Connection getConnection() {
		while (datasource.size() == 0) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return datasource.poll();
	}

	/**
	 * 5.增删改查的方法 （1）执行增删改SQL语句的方法 （2）执行查询语句的方法 （3）批量执行SQL语句的方法
	 */

	/**
	 * 执行增删改
	 * 
	 * @param sql
	 * @param params
	 * @return 影响的行数
	 */
	public int executeUpdate(String sql, Object[] params) {
		int result = 0;
		Connection conn = null;
		PreparedStatement sta = null;
		try {
			conn = getConnection();
			sta = conn.prepareStatement(sql);
			for (int i = 0; i < params.length; i++) {
				sta.setObject(i + 1, params[i]);
			}
			result = sta.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				datasource.push(conn);
			}
		}
		return result;
	}

	/**
	 * 执行查询结果
	 * 
	 * @param sql
	 * @param params
	 */
	public void executeQuery(String sql, Object[] params, QueryCallback callback) {
		Connection conn = null;
		PreparedStatement sta = null;
		ResultSet set = null;
		try {
			conn = getConnection();
			sta = conn.prepareStatement(sql);
			for (int i = 0; i < params.length; i++) {
				sta.setObject(i + 1, params[i]);
			}
			set = sta.executeQuery();
			callback.process(set);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				datasource.push(conn);
			}
		}
	}

	/**
	 * 批量执行SQL
	 * 
	 * @param sql
	 * @param paramsList
	 * @return 每条SQL影响的行数
	 */
	public int[] executeBatch(String sql, List<Object[]> paramsList) {
		int[] result = null;
		Connection conn = null;
		PreparedStatement sta = null;
		try {
			conn = getConnection();
			conn.setAutoCommit(false);
			sta = conn.prepareStatement(sql);
			for (Object[] objects : paramsList) {
				for (int i = 0; i < objects.length; i++) {
					sta.setObject(i + 1, objects[i]);
				}
				sta.addBatch();
			}
			result = sta.executeBatch();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
		return result;
	}

	/**
	 * 内部类：查询回调接口
	 * 
	 * @author work
	 * 
	 */
	public static interface QueryCallback {
		void process(ResultSet set) throws Exception;;
	}

}
