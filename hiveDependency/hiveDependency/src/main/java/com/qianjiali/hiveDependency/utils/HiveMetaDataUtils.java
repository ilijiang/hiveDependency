package com.qianjiali.hiveDependency.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetaDataUtils {

    private static final Logger logger = LoggerFactory.getLogger(HiveMetaDataUtils.class);

    public static ResultSet excuteQuery(String sql, Connection con) throws SQLException {
        logger.info("Start executing hiveSql statement");
        ResultSet res = null;
        try {
        	PreparedStatement prepareStatement = con.prepareStatement(sql);
            res = prepareStatement.executeQuery();
        } catch (SQLException e) {
            logger.error("Execution of hiveSql operation is abnormal:" + e);
            throw e;
        }
        logger.info("Execute hiveSql statement success and return");
        return res;
    }

    public static void excute(String sql, Connection con) throws SQLException {
        logger.info("Start executing hiveSql statement");
        try {
        	PreparedStatement prepareStatement = con.prepareStatement(sql);
            prepareStatement.execute();
        } catch (SQLException e) {
            logger.error("Execution of hiveSql operation is abnormal:" + e);
            throw e;
        }
        logger.info("Execute hiveSql statement success and return");
    }

    public static Connection createHiveConnection(String hiveDriver, String hiveUrl, String hiveUserName, String hivePwd) throws SQLException, ClassNotFoundException {
        Connection con = null;
        try {
            Class.forName(hiveDriver);
            con = DriverManager.getConnection(hiveUrl, hiveUserName, hivePwd);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            logger.error("create the connection to hiveSql is failed" + e);
            throw e;
        }
        return con;
    }
}