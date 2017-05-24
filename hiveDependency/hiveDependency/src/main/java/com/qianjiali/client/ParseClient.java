package com.qianjiali.client;

import com.qianjiali.hiveDependency.service.HiveParseService;

public class ParseClient{
	
	public static void main(String[] args) {
		 HiveParseService service = new HiveParseService(args);
		    try {
		      if(service.startParseScript(args)){
		        System.out.println("parse is successfully.....");
		      }else{
		        System.out.println("parse is failded....");
		      }
		    } catch (Exception e) {
		      System.out.println("startParseScript function is running exception:"+e.toString());
		    }
	}
}
