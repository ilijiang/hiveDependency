package com.qianjiali.hiveDependency.entity;

public class ScriptInfo {
	private String size;
	private String username;
	private String usergroup;
	private String userauthority;
	private String lastmotifytime;
	private String content;

	@Override
	public String toString() {
		return this.size + "|" + this.username + "|" + this.usergroup + "|" + this.userauthority + "|"
				+ this.lastmotifytime + "|" + this.content;
	}

	public String getSize() {
		return size;
	}

	public void setSize(String size) {
		this.size = size;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getUsergroup() {
		return usergroup;
	}

	public void setUsergroup(String usergroup) {
		this.usergroup = usergroup;
	}

	public String getUserauthority() {
		return userauthority;
	}

	public void setUserauthority(String userauthority) {
		this.userauthority = userauthority;
	}

	public String getLastmotifytime() {
		return lastmotifytime;
	}

	public void setLastmotifytime(String lastmofifytime) {
		this.lastmotifytime = lastmofifytime;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}
