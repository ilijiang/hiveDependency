package com.qianjiali.test;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qianjiali.hiveDependency.utils.HdfsFileUtils;
import com.qianjiali.hiveDependency.utils.HivePartitionUtils;
import com.qianjiali.hiveDependency.utils.PropertiesUtil;

public class Client {

	private static final Logger logger = LoggerFactory.getLogger(Client.class);

	static {
		PropertiesUtil.initProperties();
	}

	public static void main(String[] args) {
		prodEnv(args);
		//testEnv();
	}

	public static void testEnv() {
		try {
			PropertiesUtil.setHiveParsePartition("20170421");
			HdfsFileUtils.parseHiveAndUpdate2HiveByScriptAddress("/user/qianjiali/edw_houses/ADM/DDL/test");
			
			//TODO 
			HdfsFileUtils.updateScriptContext();
			
			HivePartitionUtils.createNewPartition();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (HdfsFileUtils.fileSystem != null) {
					HdfsFileUtils.fileSystem.close();
				}
			} catch (IOException e) {
				System.out.println(e.toString());
				e.printStackTrace();
			}
		}
	}

	public static void prodEnv(String[] args) {
		try {
			if (args.length == 2) {// 自定义分区
				PropertiesUtil.setHiveParsePartition(args[1]);
			}
			logger.info("The incoming address is " + args[0]);
			System.out.println("The incoming address is " + args[0]);
			HdfsFileUtils.parseHiveAndUpdate2HiveByScriptAddress(args[0]);
			HivePartitionUtils.createNewPartition();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (HdfsFileUtils.fileSystem != null) {
					HdfsFileUtils.fileSystem.close();
				}
			} catch (IOException e) {
				System.out.println(e.toString());
				e.printStackTrace();
			}
		}
	}

}
