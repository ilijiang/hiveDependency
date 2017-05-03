package com.fangdd.datamanager.DB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.fangdd.datamanager.entity.QueryItem;
import com.fangdd.datamanager.utils.QuerySqlCreateUtils;
import com.sun.xml.internal.xsom.impl.scd.Iterators.Map;

public class DBHelp {
	
	/**
	 * mysql connection info
	 */
	private static String hiveDriver = "com.mysql.jdbc.Driver";
	private static String hiveUrl = "jdbc:mysql://10.12.21.131:3306/fangdd_esf_data?useUnicode=true&amp;characterEncoding=utf-8&amp;zeroDateTimeBehavior=convertToNull&amp;autoReconnect=true&amp;allowMultiQueries=true";
	private static String hiveUserName = "root";
	private static String hivePwd = "123456";
	
	
	static {
		try {
			Class.forName(hiveDriver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static Connection createHiveConnection() throws Exception {
		Connection con = null;
		try {
			con = DriverManager.getConnection(hiveUrl, hiveUserName, hivePwd);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return con;
	}

	public static ResultSet excuteQuery(QueryItem queryItem) throws Exception {
		System.out.println("Start executing hiveSql statement");
		ResultSet res = null;
		Connection con = null;
		PreparedStatement prepareStatement = null;
		try {
			con = ConnectionPool.getDBConnection();
			prepareStatement = con.prepareStatement(QuerySqlCreateUtils.getQuerySql(queryItem));
			res = prepareStatement.executeQuery();
		} catch (SQLException e) {
			System.out.println("Execution of hiveSql operation is abnormal:" + e);
			throw e;
		} finally {
			if(con!=null){
				ConnectionPool.closeDBConnection(con);
			}
		}
		System.out.println("Execute hiveSql statement success and return");
		return res;
	}
}













