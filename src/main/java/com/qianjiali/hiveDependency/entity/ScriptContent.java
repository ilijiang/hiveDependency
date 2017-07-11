package com.qianjiali.hiveDependency.entity;

import java.util.HashMap;
import java.util.Map;

public class ScriptContent {
	/**
	 * key scriptname value scriptcontext
	 */
	public static Map<String, ScriptInfo> scriptMap = new HashMap<String, ScriptInfo>();

	private ScriptContent() {
	}

	public static String getScriptConent(){
	   StringBuilder builder = new StringBuilder();
	   for(Map.Entry<String,ScriptInfo> entry:scriptMap.entrySet()){
		   String scriptname = entry.getKey();
		   ScriptInfo scriptInfo = entry.getValue();
		   builder.append(scriptname+"|"+scriptInfo.toString());
		   builder.append("\n");
	   }
	   return builder.toString();
   }
}
