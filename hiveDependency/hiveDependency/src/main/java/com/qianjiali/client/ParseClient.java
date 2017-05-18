package com.qianjiali.client;

import com.qianjiali.hiveDependency.service.HiveParseService;

public class ParseClient{
	
	public static void main(String[] args) {
		HiveParseService service = new HiveParseService();
		service.startParseScript(args);
	}
}
