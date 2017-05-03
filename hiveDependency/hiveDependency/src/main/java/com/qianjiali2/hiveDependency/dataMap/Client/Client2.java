package com.qianjiali2.hiveDependency.dataMap.Client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qianjiali.hiveDependency.utils.HdfsFileUtils;
import com.qianjiali.hiveDependency.utils.PropertiesUtil;
import com.qianjiali2.hiveDependency.dataMap.utils.HdfsFileUtils2;

public class Client2 {
	
	private static final Logger logger = LoggerFactory.getLogger(Client2.class);
	
	static{
		PropertiesUtil.initProperties();
	}
    
    public static void main(String[] args) {
    	//demo(args);
    	HdfsFileUtils.parseHiveAndUpdate2HiveByScriptAddress("/user/qianjiali/edw_houses/ADM/DDL/test");//
    }
    
    public static void demo(String[] args){
    	try {
    		logger.info("The incoming address is "+args[0]);
    		System.out.println("The incoming address is "+args[0]);
    		HdfsFileUtils2.parseHiveAndUpdate2HiveByScriptAddress(args[0]);///user/qianjiali/edw_houses/ADM/DDL/test
    		System.out.println("解析完成.....");
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
   
}
