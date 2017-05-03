package com.qianjiali.hiveDependency.entity;

import java.util.Date;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter.DEFAULT;

import com.qianjiali.hiveDependency.utils.DateUtil;

public class HiveConfig {
	
	private String partitionStr = DateUtil.DateToString(new Date(), DateUtil.YYYYMMDD);

	// hdfs地址
	private String hdfsConfig;

	/**
	 * hdfs脚本存放的路径
	 */
	private String hdfsDir;

	/**
	 * 写入映射信息的路径
	 */
	private String hdfsDest;
	
	
	private String patternComments;
	private String patternInsert ;
	private String patternPartition;
	
	private String hiveDriver;
	private String hiveUrl;
	private String hiveUserName;
	private String hivePassword;
	
	private String createNewPartitionSql;
	private String deletePartitionSql;
	
	private String createScriptDetailPartitionSql;
	private String deleteScriptDetailPartitionSql;
	
	
	private String scriptContentHdfsWriteUrl;

	public String getHdfsConfig() {
		return hdfsConfig;
	}

	public void setHdfsConfig(String hdfsConfig) {
		this.hdfsConfig = hdfsConfig;
	}

	public String getHdfsDir() {
		return hdfsDir;
	}

	public void setHdfsDir(String hdfsDir) {
		this.hdfsDir = hdfsDir;
	}

	public String getHdfsDest() {
		return hdfsDest+"/"+getPartitionStr();
	}

	public void setHdfsDest(String hdfsDest) {
		this.hdfsDest = hdfsDest;
	}

	public static HiveConfig getInstance() {
		return ConfSingleton.hiveParseConfig;
	}

	static class ConfSingleton {
		public final static HiveConfig hiveParseConfig = new HiveConfig();
	}

	private HiveConfig(){}

	public String getPatternComments() {
		return patternComments;
	}

	public void setPatternComments(String patternComments) {
		this.patternComments = patternComments;
	}

	public String getPatternInsert() {
		return patternInsert;
	}

	public void setPatternInsert(String patternInsert) {
		this.patternInsert = patternInsert;
	}

	public String getPatternPartition() {
		return patternPartition;
	}

	public void setPatternPartition(String patternPartition) {
		this.patternPartition = patternPartition;
	}

	public String getHiveDriver() {
		return hiveDriver;
	}

	public void setHiveDriver(String hiveDriver) {
		this.hiveDriver = hiveDriver;
	}

	public String getHiveUrl() {
		return hiveUrl;
	}

	public void setHiveUrl(String hiveUrl) {
		this.hiveUrl = hiveUrl;
	}

	public String getHiveUserName() {
		return hiveUserName;
	}

	public void setHiveUserName(String hiveUserName) {
		this.hiveUserName = hiveUserName;
	}

	public String getHivePassword() {
		return hivePassword;
	}

	public void setHivePassword(String hivePassword) {
		this.hivePassword = hivePassword;
	}
	
	public String getPartitionStr(){
		return partitionStr;
	}

	public void setPartitionStr(String partitionStr) {
		this.partitionStr = partitionStr;
	}

	public String getCreateNewPartitionSql() {
		return createNewPartitionSql;
	}

	public void setCreateNewPartitionSql(String createNewPartitionSql) {
		this.createNewPartitionSql = createNewPartitionSql;
	}

	public String getDeletePartitionSql() {
		return deletePartitionSql;
	}

	public void setDeletePartitionSql(String deletePartitionSql) {
		this.deletePartitionSql = deletePartitionSql;
	}

	public String getScriptContentHdfsWriteUrl() {
		return scriptContentHdfsWriteUrl+"/"+getPartitionStr()+"/"+getPartitionStr()+".txt";
	}

	public void setScriptContentHdfsWriteUrl(String scriptContentHdfsWriteUrl) {
		this.scriptContentHdfsWriteUrl = scriptContentHdfsWriteUrl;
	}

	public String getCreateScriptDetailPartitionSql() {
		return createScriptDetailPartitionSql;
	}

	public void setCreateScriptDetailPartitionSql(String createScriptDetailPartitionSql) {
		this.createScriptDetailPartitionSql = createScriptDetailPartitionSql;
	}

	public String getDeleteScriptDetailPartitionSql() {
		return deleteScriptDetailPartitionSql;
	}

	public void setDeleteScriptDetailPartitionSql(String deleteScriptDetailPartitionSql) {
		this.deleteScriptDetailPartitionSql = deleteScriptDetailPartitionSql;
	}
}
