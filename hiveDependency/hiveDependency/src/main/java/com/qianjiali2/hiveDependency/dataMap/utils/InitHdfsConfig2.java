package com.qianjiali2.hiveDependency.dataMap.utils;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class InitHdfsConfig2 {

	public static FileSystem loadHdfsFileConf(String hdfsConfigPath) throws Exception {
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(getConfiguration(hdfsConfigPath));
			HdfsFileUtils2.fileSystem = fileSystem;
		} catch (Exception e) {
			throw e;
		}
		return fileSystem;
	}

	private static Configuration getConfiguration(String hdfsConfigPath) throws MalformedURLException {
		System.out.println("初始化hdfs配置资源..........");
		Configuration conf = new Configuration();
		for (String resource : hdfsConfigPath.split(",")) {
			conf.addResource(new URL(resource));
		}
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		return conf;
	}

}
