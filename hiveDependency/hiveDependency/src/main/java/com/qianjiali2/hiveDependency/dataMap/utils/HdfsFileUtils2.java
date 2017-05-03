package com.qianjiali2.hiveDependency.dataMap.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qianjiali.hiveDependency.entity.HiveConfig;
import com.qianjiali.hiveDependency.entity.ParseNode;
import com.qianjiali.hiveDependency.entity.ScriptItem;
import com.qianjiali.hiveDependency.service.HiveParseTask;
import com.qianjiali.hiveDependency.utils.HdfsFileUtils;

public class HdfsFileUtils2 {

	public static FileSystem fileSystem;

	private static final Logger logger = LoggerFactory.getLogger(HdfsFileUtils2.class);
	
	private static HiveConfig hiveParseConfig = HiveConfig.getInstance();
	
	private static ExecutorService taskPool = Executors.newFixedThreadPool(5);

	static {
		try {
			fileSystem = InitHdfsConfig2.loadHdfsFileConf(hiveParseConfig.getHdfsConfig());
			System.out.println("hdfs资源初始化配置完成..........");
		} catch (Exception e) {
			logger.error("Initialize hdfs environment failed" + e);
			e.printStackTrace();
		}
	}

	/**
	 * 返回指定目录下脚本文件名（hdfs上脚本的路径）
	 * @param path
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static List<Path> listFile(Path path) throws FileNotFoundException, IOException {
		List<Path> paths = new ArrayList<Path>();
		if (!fileSystem.exists(path)) {
			return paths;
		}
		if (fileSystem.isFile(path)) {
			paths.add(path);
			return paths;
		} else {
			RemoteIterator<LocatedFileStatus> subFiles = fileSystem.listFiles(path, true);
			while (subFiles.hasNext()) {
				Path childPath = subFiles.next().getPath();
				if (fileSystem.isFile(childPath)) {
					paths.add(childPath);
				}
			}
			logger.info("The number of scripts under "+path.toString()+" is "+paths.size());
			System.out.println("The number of scripts under "+path.toString()+" is "+paths.size());
			return paths;
		}
	}

	@SuppressWarnings("all")
	public static void downloadScriptForHdfs(String urlPath, String dest) throws Exception {
		InputStream is = null;
		FileOutputStream fileout = null;
		try {
			HttpClient client = new DefaultHttpClient();
			HttpGet httpget = new HttpGet(urlPath);
			HttpResponse response = client.execute(httpget);

			HttpEntity entity = response.getEntity();
			is = entity.getContent();

			File file = new File(dest);
			file.getParentFile().mkdirs();
			fileout = new FileOutputStream(file);
			/**
			 * 根据实际运行效果 设置缓冲区大小
			 */
			byte[] buffer = new byte[10 * 1024];
			int ch = 0;
			while ((ch = is.read(buffer)) != -1) {
				fileout.write(buffer, 0, ch);
			}
			fileout.flush();
		} catch (Exception e) {
			logger.error("Downloading script from hdfs failed" + e);
			throw e;
		} finally {
			try {
				if (is != null) {
					is.close();
				}
				if (fileout != null) {
					fileout.close();
				}
			} catch (IOException e) {
				logger.error("close fileoutputstream stream is  failed" + e);
				throw e;
			}
		}
	}

	public static String UploadFile(String urlPath, String dest) {
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(urlPath));
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(dest), conf);
			OutputStream out = fs.create(new Path(dest), new Progressable() {
				public void progress() {
					System.out.print(".");
				}
			});
			IOUtils.copyBytes(in, out, 4096, true);
		} catch (Exception e) {
			logger.error("Upload data map file to hdfs failed" + e);
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * read the hdfs file content notice that the dst is the full path name
	 */
	public static byte[] readHDFSFile(String dst) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(dst);
		if (fs.exists(path)) {
			FSDataInputStream is = fs.open(path);
			// get the file info to create the buffer
			FileStatus stat = fs.getFileStatus(path);
			// create the buffer
			byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
			is.readFully(0, buffer);

			is.close();
			fs.close();
			return buffer;
		} else {
			throw new Exception("the file is not found .");
		}
	}

	public static void closeHdfs(FSDataInputStream fin, FSDataOutputStream fout, BufferedReader reader)
			throws Exception {
		try {
			if (fin != null) {
				fin.close();
			}
			if (reader != null) {
				reader.close();
			}
			if (fout != null) {
				fout.close();
			}
//			if (fileSystem != null) {
//				fileSystem.close();
//			}
		} catch (Exception e) {
			logger.error("close hdfs stream failed" + e);
			throw e;
		}
	}

	/**
	 * 通过传入分析脚本和脚本名称实现脚本的依赖解析以及上传至Hive数据库
	 * @param scriptStr 脚本字符串
	 * @param scriptName 脚本名称
	 * @throws Exception
	 */
	public static void parseHiveAndUpdate2HiveByScriptStr(String scriptStr, String scriptName) throws Exception {
		FSDataOutputStream fout = null;
		try {
			String relationWriteUrl = hiveParseConfig.getHdfsDest()+"/"+scriptName+".txt";
			Path dest = new Path(relationWriteUrl);
			fout = fileSystem.create(dest, true);
			ScriptItem item = new ScriptItem(scriptStr);
			ParseNode node = item.getParseNode();
			String noderelation = TxtFileUtils2.getHiveNodeRelation(node, scriptName);
			fout.write(noderelation.getBytes(), 0, noderelation.getBytes().length);
			fout.flush();
		} catch (Exception e) {
			logger.error("HdfsFileUtils.parseHiveAndUpdate2HiveByScriptStr running is failed" + e);
			throw e;
		} finally {
			closeHdfs(null, fout, null);
		}
	}

	/**
	 * 通过传入脚本存放地址，实现脚本的依赖解析，并上传至Hive数据库
	 * @param ScriptAddress
	 * @throws Exception
	 */
	public static void parseHiveAndUpdate2HiveByScriptAddress(String ScriptAddress) {
		Path scriptPath = new Path(ScriptAddress);
		List<Path> list = new ArrayList<Path>();
		try {
			list = HdfsFileUtils.listFile(scriptPath).stream().filter(path->filterParseScriptName(path.getName())).collect(Collectors.toList());
			int count = list.size();
			CountDownLatch cdl = new CountDownLatch(count);
			for (Path path : list) {
				taskPool.execute(new HiveParseTask(fileSystem, path,cdl));
			}
			cdl.await();
		} catch (Exception e) {
			System.out.println("parseHiveAndUpdate2HiveByScriptAddress function error:"+e.toString());
			e.printStackTrace();
		}
		taskPool.shutdown();
	}
	

	private static boolean filterParseScriptName(String scriptName){
        if(scriptName.endsWith(".sql")){
        	if(scriptName.endsWith("_create.sql")){
        		return false;
        	}else if(scriptName.startsWith("_ic")||scriptName.startsWith("edw_")){
        		return true;
        	}else{
        		return false;
        	}
        }else if(scriptName.endsWith(".hsql")){
        	return true;
        }else{
        	return false;
        }
	} 
}



















