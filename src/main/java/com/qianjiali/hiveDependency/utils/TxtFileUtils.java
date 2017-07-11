package com.qianjiali.hiveDependency.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.qianjiali.hiveDependency.entity.HiveConfig;
import com.qianjiali.hiveDependency.entity.ParseNode;

public class TxtFileUtils {

	public static void CreateTxtFile(String filePath) throws Exception {
		File file = new File(filePath);
		try {
			if (file.exists() && file.isFile()) {
				file.delete();
			}
			file.getParentFile().mkdirs();
			file.createNewFile();
		} catch (Exception e) {
			throw e;
		}
	}

	/***
	 * 获取指定目录下的所有的文件（不包括文件夹），采用了递归 获取本地的Sql文件
	 * 
	 * @param obj
	 * @return
	 */
	public static List<File> getLocalCacheScript(Object obj) {
		File file = null;
		if (obj instanceof File) {
			file = (File) obj;
		} else {
			file = new File(obj.toString());
		}
		List<File> files = new ArrayList<File>();
		if (file.isFile() && file.getName().endsWith("sql")) {
			files.add(file);
			return files;
		} else if (file.isDirectory()) {
			File[] fileArr = file.listFiles();
			for (int i = 0; i < fileArr.length; i++) {
				File fileOne = fileArr[i];
				files.addAll(getLocalCacheScript(fileOne));
			}
		}
		return files;
	}

	public static void WriteTxtFile(String newStrs, String filePath) throws Exception {
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(filePath, true);
			fileWriter.write(newStrs);
			fileWriter.flush();
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				if (fileWriter != null) {
					fileWriter.close();
				}
			} catch (IOException e) {
				throw e;
			}
		}
	}

	public static String getHiveNodeRelation(ParseNode parseNode, String scriptName) throws Exception {
		StringBuilder scriptMap = new StringBuilder();
		try {
			if (parseNode.getTableTargetObjects().size() != 0 && parseNode.getTableSourceObjects().size() != 0) {
				for (String sourceTable : parseNode.getTableSourceObjects()) {
					for (String targetTable : parseNode.getTableTargetObjects()) {
						String[] sR = sourceTable.split("\t");
						String[] tR = targetTable.split("\t");
						scriptMap.append(scriptName + "|" + sR[0] + "|" + tR[0] + "|" + tR[1] + "|TBD" + "|"
								+ DateUtil.DateToString(new Date(),DateUtil.YYYYMMDDHHMMSS) + "\r\n");
					}
				}
			} else if (parseNode.getTableTargetObjects().size() == 0 && parseNode.getTableSourceObjects().size() != 0) {
				for (String sourceTable : parseNode.getTableSourceObjects()) {
					String[] sR = sourceTable.split("\t");
					scriptMap.append(scriptName + "|" + sR[0] + "|" + "?" + "|" + "?" + "|TBD" + "|"
							+ DateUtil.DateToString(new Date(),DateUtil.YYYYMMDDHHMMSS)  + "\r\n");
				}
			} else if (parseNode.getTableTargetObjects().size() != 0 && parseNode.getTableTargetObjects().size() == 0) {
				for (String targetTable : parseNode.getTableTargetObjects()) {
					String[] tR = targetTable.split("\t");
					scriptMap.append(scriptName + "|" + "?" + "|" + tR[0] + "|" + tR[1] + "|TBD" + "|"
							+ DateUtil.DateToString(new Date(),DateUtil.YYYYMMDDHHMMSS)  + "\r\n");
				}
			}
		} catch (Exception e) {
			throw e;
		}
		return scriptMap.toString();
	}
}
