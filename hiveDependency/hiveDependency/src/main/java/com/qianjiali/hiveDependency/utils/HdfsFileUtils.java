package com.qianjiali.hiveDependency.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qianjiali.hiveDependency.entity.HiveConfig;
import com.qianjiali.hiveDependency.entity.ParseNode;
import com.qianjiali.hiveDependency.entity.ScriptContent;
import com.qianjiali.hiveDependency.entity.ScriptInfo;
import com.qianjiali.hiveDependency.entity.ScriptItem;
import com.qianjiali.hiveDependency.service.HiveParseTask;

import jline.internal.InputStreamReader;

public class HdfsFileUtils {

	public static FileSystem fileSystem;

	private static final Logger logger = LoggerFactory.getLogger(HdfsFileUtils.class);

	private static HiveConfig hiveParseConfig = HiveConfig.getInstance();

	private static ExecutorService taskPool = Executors.newFixedThreadPool(5);

	static {
		try {
			fileSystem = InitHdfsConfig.loadHdfsFileConf(hiveParseConfig.getHdfsConfig());
			System.out.println("hdfs资源初始化配置完成..........");
		} catch (Exception e) {
			logger.error("Initialize hdfs environment failed" + e);
			e.printStackTrace();
		}
	}

	/**
	 * 返回指定目录下脚本文件名（hdfs上脚本的路径）
	 * 
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
				if (fileSystem.isFile(childPath)&&(childPath.getName().endsWith(".sql")||childPath.getName().endsWith(".hsql"))) {
					paths.add(childPath);
				}
			}
			logger.info("The number of scripts under " + path.toString() + " is " + paths.size());
			System.out.println("The number of scripts under " + path.toString() + " is " + paths.size());
			return paths;
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
			// if (fileSystem != null) {
			// fileSystem.close();
			// }
		} catch (Exception e) {
			logger.error("close hdfs stream failed" + e);
			throw e;
		}
	}

	/**
	 * 通过传入分析脚本和脚本名称实现脚本的依赖解析以及上传至Hive数据库
	 * 
	 * @param scriptStr
	 *            脚本字符串
	 * @param scriptName
	 *            脚本名称
	 * @throws Exception
	 */
	public static void parseHiveAndUpdate2HiveByScriptStr(String scriptStr, String scriptName) throws Exception {
		FSDataOutputStream fout = null;
		try {
			String relationWriteUrl = hiveParseConfig.getHdfsDest() + "/" + scriptName + ".txt";
			Path dest = new Path(relationWriteUrl);
			fout = fileSystem.create(dest, true);
			ScriptItem item = new ScriptItem(scriptStr);
			ParseNode node = item.getParseNode();
			String noderelation = TxtFileUtils.getHiveNodeRelation(node, scriptName);
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
	 * 
	 * @param ScriptAddress
	 * @throws Exception
	 */
	public static void parseHiveAndUpdate2HiveByScriptAddress(String ScriptAddress) {
		Path scriptPath = new Path(ScriptAddress);
		try {
			List<Path> allScriptFile = HdfsFileUtils.listFile(scriptPath);
			setScriptContextInfo(allScriptFile);
			List<Path> filterScriptFile = allScriptFile.stream().filter(path->filterParseScriptName(path.getName().trim().toLowerCase())).collect(Collectors.toList());
			int count = filterScriptFile.size();
			CountDownLatch cdl = new CountDownLatch(count);
			for (Path path : filterScriptFile) {
				taskPool.execute(new HiveParseTask(path,cdl));
			}
			cdl.await();
		} catch (Exception e) {
			System.out.println("parseHiveAndUpdate2HiveByScriptAddress function error:"+e.toString());
			e.printStackTrace();
		} finally{
			taskPool.shutdown();
		}
	
	}
	
	public static List<String> getFileCotentByPath(Path path){
		FSDataInputStream fin = null;
		BufferedReader reader = null;
		List<String> parseScripts = new ArrayList<String>();
		try {
			fin = fileSystem.open(path);
			reader = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
			parseScripts = HiveScriptFilterUtils.filterScriptLine(reader);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				HdfsFileUtils.closeHdfs(fin, null, reader);
			} catch (Exception e) {
				System.out.println("close the hdfs stream is error:" + e.toString());
			}
		}
		return parseScripts;
	}
	
	public static void writeFile2Hdfs(String writeUrl,String writeContent){
		System.out.println("start write file to hdfs,url is:"+writeUrl);
		FSDataOutputStream fout = null;
		try {
			System.out.println("update the script content of url:"+writeUrl);
			Path dest = new Path(writeUrl);
			fout = fileSystem.create(dest, true);
			fout.write(writeContent.getBytes(), 0, writeContent.getBytes().length);
			fout.flush();
		} catch (Exception e) {
			System.out.println("update the script content is error"+ e.toString());
		} finally {
			try {
				HdfsFileUtils.closeHdfs(null, fout, null);
			} catch (Exception e) {
				System.out.println("close the hdfs stream is error:"+e.toString());
			}
		}
		System.out.println("the file write is sucessesfully");
	}
	
	public static void updateScriptContext(){
		System.out.println("start update the script content");
		try {
			writeFile2Hdfs(hiveParseConfig.getScriptContentHdfsWriteUrl(), ScriptContent.getScriptConent());
			fileSystem.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("the script content update is sucessesfully");
	}

	private static boolean filterParseScriptName(String scriptName) {
		
		
//		if(scriptName.contains("_backup")){
//			return false;
//		}
//		
//		if(scriptName.endsWith("_create.sql")){
//			return false;
//		}
//		
//		if(scriptName.startsWith("edw_applications")){
//			return false;
//		}
//		
//		if (!scriptName.endsWith(".sql")&&!scriptName.endsWith(".hsql")){
//			return false;
//		}
//				
//		if((scriptName.startsWith("ic_")||scriptName.startsWith("edw_"))){
//			return true;
//		}
		
		if (scriptName.endsWith(".sql")||scriptName.endsWith(".hsql")){
			if((scriptName.startsWith("ic_")||scriptName.startsWith("edw_"))){
				return true;
			}else{
				return false;
			}
		}else{
			return false;
		}
				
		
		
		
		
		
//		if ((!scriptName.contains("_backup")) &&(!scriptName.endsWith("_create.sql")) && scriptName.endsWith(".sql")) {
//			if (scriptName.startsWith("_ic")
//					|| ((!scriptName.startsWith("edw_applications")) && scriptName.startsWith("edw_"))) {
//				return true;
//			} else {
//				return false;
//			}
//		} else if (scriptName.endsWith(".hsql")) {
//			return true;
//		} else {
//			return false;
//		}
	}
	

	private static void setScriptContextInfo(List<Path> allPath) {
		FSDataInputStream fin = null;
		BufferedReader reader = null;
		for(Path path:allPath){
			try {
				ScriptInfo scriptInfo = new ScriptInfo();
				String scriptname = path.getName();
				fin = fileSystem.open(path);
				reader = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				FileStatus fileStatus = HdfsFileUtils.fileSystem.getFileStatus(path);
				scriptInfo.setSize(Double.valueOf(fileStatus.getLen()/1000.0)+"KB");
				scriptInfo.setLastmotifytime(DateUtil.LongToDateString(fileStatus.getModificationTime()));
				scriptInfo.setUsername(fileStatus.getOwner());
				scriptInfo.setUsergroup(fileStatus.getGroup());
				scriptInfo.setUserauthority(String.valueOf(fileStatus.getPermission().toShort()));
				scriptInfo.setContent(HiveScriptFilterUtils.firstFilterScript(reader).replaceAll("\n"," "));
				ScriptContent.scriptMap.put(scriptname, scriptInfo);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					HdfsFileUtils.closeHdfs(fin, null, reader);
				} catch (Exception e) {
					System.out.println("close the hdfs stream is error:" + e.toString());
				}
			}
		}
		System.out.println("ScriptContent number of ===>>>>"+ScriptContent.scriptMap.size());
	}
}
