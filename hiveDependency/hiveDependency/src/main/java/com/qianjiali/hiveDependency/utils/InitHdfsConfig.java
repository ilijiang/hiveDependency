package com.qianjiali.hiveDependency.utils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsUtils;

public class InitHdfsConfig {

	public static FileSystem loadHdfsFileConf(String hdfsConfigPath) throws Exception {
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(getConfiguration(hdfsConfigPath));
			HdfsFileUtils.fileSystem = fileSystem;
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

	public static FSDataInputStream getJarConfiguration(String hdfsConfigUrl) throws IOException {
		System.out.println("初始化hdfs配置资源..........");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(hdfsConfigUrl),conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(hdfsConfigUrl));
		return hdfsInStream;
	}
}
