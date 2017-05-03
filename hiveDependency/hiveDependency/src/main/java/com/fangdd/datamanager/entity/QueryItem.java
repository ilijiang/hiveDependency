package com.fangdd.datamanager.entity;

import java.io.Serializable;

public class QueryItem implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	private String script;
	private String starttime;

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public String getStarttime() {
		return starttime;
	}

	public void setStarttime(String starttime) {
		this.starttime = starttime;
	}

	public QueryItem(String script, String starttime) {
		this.script = script;
		this.starttime = starttime;
	}
	public QueryItem() {}
}
