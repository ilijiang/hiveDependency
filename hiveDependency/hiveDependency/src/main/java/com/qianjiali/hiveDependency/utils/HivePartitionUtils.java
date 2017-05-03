package com.qianjiali.hiveDependency.utils;

import java.sql.Connection;
import java.text.MessageFormat;

import com.qianjiali.hiveDependency.entity.HiveConfig;

public class HivePartitionUtils {

	private static HiveConfig hiveConfig = HiveConfig.getInstance();
	
	public static void createNewPartition() throws Exception{
		System.out.println("\n\nStart creating a new partition:"+hiveConfig.getPartitionStr());
		Connection connection = null;
		try {
			connection = HiveMetaDataUtils.createHiveConnection(hiveConfig.getHiveDriver(),
					hiveConfig.getHiveUrl(), hiveConfig.getHiveUserName(), hiveConfig.getHivePassword());
			createScriptParsePartition(connection);
			createScriptDetailPartition(connection);
			System.out.println("New partition created successfully\n\n");
		} catch (Exception e) {
			System.out.println("New partition created failed\n\n");
			throw e;
		} finally {
			try {
				if(connection!=null){
					connection.close();
				}
			} catch (Exception e) {
				throw e;
			}
		}
	}
	
	/**
	 * 
	 * @param connection
	 * @throws Exception
	 */
	private static void createScriptParsePartition(Connection connection) throws Exception{
		System.out.println("\n\nStartting a new script parse partition:"+hiveConfig.getPartitionStr());
		try {
			String createNewPartitionSql = MessageFormat.format(hiveConfig.getCreateNewPartitionSql(),hiveConfig.getPartitionStr());
			String deletePartitionSql = MessageFormat.format(hiveConfig.getDeletePartitionSql(),hiveConfig.getPartitionStr());
			System.out.println("Partition delete script is=============>>>"+deletePartitionSql);
			HiveMetaDataUtils.excute(deletePartitionSql, connection);
			System.out.println("Create a new partition script is==========>>>"+createNewPartitionSql);
			HiveMetaDataUtils.excute(createNewPartitionSql, connection);
			System.out.println("New partition created successfully\n\n");
		} catch (Exception e) {
			System.out.println("New partition created failed\n\n");
			throw e;
		} 
	}
	
	private static void createScriptDetailPartition(Connection connection) throws Exception{
		System.out.println("\n\nStart creating a new script detail info partition:"+hiveConfig.getPartitionStr());
		try {
			String createNewPartitionSql = MessageFormat.format(hiveConfig.getCreateScriptDetailPartitionSql(),hiveConfig.getPartitionStr());
			String deletePartitionSql = MessageFormat.format(hiveConfig.getDeleteScriptDetailPartitionSql(),hiveConfig.getPartitionStr());
			System.out.println("Script detail Partition delete script is=============>>>"+deletePartitionSql);
			HiveMetaDataUtils.excute(deletePartitionSql, connection);
			System.out.println("Create a new partition Script detail  is==========>>>"+createNewPartitionSql);
			HiveMetaDataUtils.excute(createNewPartitionSql, connection);
			System.out.println("New Script detail partition created successfully\n\n");
		} catch (Exception e) {
			System.out.println("New Script detail partition created failed\n\n");
			throw e;
		}
	}

	public static void alterNewPartition(String oldPartitiion, String newPartitiion) {

	}

	public static void deleteNewPartition(String oldPartitiion, String newPartitiion) {

	}

}
