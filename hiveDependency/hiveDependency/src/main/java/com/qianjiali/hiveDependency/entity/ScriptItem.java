package com.qianjiali.hiveDependency.entity;

import com.qianjiali.hiveDependency.utils.HiveParseUtils;

public class ScriptItem {
	
	private String scriptStr;
	
	private HiveParseUtils parse;

	public ScriptItem(String scriptStr) {
		super();
		this.scriptStr = scriptStr;
		this.parse = new HiveParseUtils(new ParseNode(),scriptStr);
	}

	public String getScriptStr() {
		return scriptStr;
	}

	public ParseNode getParseNode() throws Exception {
		try {
			return parse.parse();
		} catch (Exception e) {
			throw e;
		}
	}
}
