package com.fangdd.datamanager.utils;

import java.net.InetSocketAddress;

import com.fangdd.datamanager.RPC.RpcImporter;
import com.fangdd.datamanager.service.IExecuteHistoryService;
import com.fangdd.datamanager.serviceImpl.ExecuteHistoryServiceImpl;

public class DatamanagerServiceFactory {
	
    public static IExecuteHistoryService createDatamanagerService(){
    	IExecuteHistoryService  queryservice = null;
    	try {
			RpcImporter<IExecuteHistoryService> importer = new RpcImporter<IExecuteHistoryService>();
			InetSocketAddress inetSocketAddress = new InetSocketAddress("10.12.12.2", 8888);
			queryservice = importer.importer(ExecuteHistoryServiceImpl.class,inetSocketAddress);
			return queryservice;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.toString());
		}
    	return queryservice;
    }
}
