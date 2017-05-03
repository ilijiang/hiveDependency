package com.fangdd.datamanager.client;

import java.util.List;

import com.fangdd.datamanager.entity.User;
import com.fangdd.datamanager.utils.DatamanagerServiceFactory;

public class RPCClient {
	
	public static void main(String[] args) {
		transferRpcService();
	}
	public static void transferRpcService(){
		try {
			List<User> users = DatamanagerServiceFactory.createDatamanagerService().getUserExcuteHistoryByscriptStr("select user_trace.actiontime, user_trace.event, user_trace.prepagename");
			System.out.println(users.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
