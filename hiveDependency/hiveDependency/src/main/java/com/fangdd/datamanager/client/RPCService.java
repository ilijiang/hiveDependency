package com.fangdd.datamanager.client;

import com.fangdd.datamanager.RPC.RpcExporter;

public class RPCService {

	public static void main(String[] args) {
		startRpcService();
	}
	
	public static void startRpcService(){
		try {
			RpcExporter.export("10.12.12.2", 8888);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
