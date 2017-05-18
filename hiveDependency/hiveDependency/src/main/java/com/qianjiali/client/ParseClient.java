package com.qianjiali.client;

import com.qianjiali.hiveDependency.service.HiveParseService;

public class ParseClient{
	
	public static void main(String[] args) {
		HiveParseService service = new HiveParseService();
		try {
			service.startParseScript(args);
		} catch (Exception e) {
			System.out.println("startParseScript function is running fail:"+e.toString());
		}
	}
}
