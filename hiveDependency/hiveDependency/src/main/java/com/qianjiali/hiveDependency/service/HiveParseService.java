package com.qianjiali.hiveDependency.service;

import java.io.IOException;

import com.qianjiali.hiveDependency.entity.HiveConfig;
import com.qianjiali.hiveDependency.utils.HdfsFileUtils;
import com.qianjiali.hiveDependency.utils.HivePartitionUtils;
import com.qianjiali.hiveDependency.utils.PropertiesUtil;

public class HiveParseService {

	public void startParseScript(String[] args){
		
		HiveConfig conf = PropertiesUtil.initProperties();
		
		System.out.println("开始执行解析...............");
		
		if(conf==null){
			System.out.println("conf is null...............");
		}
		
		if(conf.getRunningEnv()==null){
			System.out.println("conf running env is null...............");
		}
		
		//testEnv();
		
		if(conf.getRunningEnv().equals("test")){
			testEnv();
		}else if(conf.getRunningEnv().equals("prod")){
			prodEnv(args);
		}else{
			System.out.println("no running env......");
		}
	}

	public  void testEnv() {
		try {
			PropertiesUtil.setHiveParsePartition("20170425");
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

	public void prodEnv(String[] args) {
		try {
			if (args.length == 2) {// 自定义分区
				PropertiesUtil.setHiveParsePartition(args[1]);
			}
			System.out.println("The incoming address is " + args[0]);
			HdfsFileUtils.parseHiveAndUpdate2HiveByScriptAddress(args[0]);
			
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

}
