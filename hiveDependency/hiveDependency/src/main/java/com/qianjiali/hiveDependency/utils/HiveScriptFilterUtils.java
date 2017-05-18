package com.qianjiali.hiveDependency.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.qianjiali.hiveDependency.entity.HiveConfig;
import com.qianjiali.hiveDependency.entity.ScriptContent;

import jline.internal.InputStreamReader;

public class HiveScriptFilterUtils {
	
	public static void analysisMultipleScripts(String scriptStr, List<String> scripts) {
		Pattern pinsert = Pattern.compile(HiveConfig.getInstance().getPatternInsert(),
				Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		Matcher m = pinsert.matcher(scriptStr.toLowerCase().trim());
		if (!m.find()) {
			if(!scriptStr.toLowerCase().startsWith("create")){
				if (scriptStr.trim().charAt(scriptStr.length() - 1) == ';') {// 避免最后一条执行语句的最后出现分号
					scriptStr = scriptStr.substring(0, scriptStr.length() - 1).trim();
				}
				scripts.add(scriptStr);
			}
		} else {
			String str = m.group();
			int firstIndex = scriptStr.indexOf(str);
			int endIndex = firstIndex + str.length();
			String[] result = new String[2];
			result[0] = scriptStr.substring(0, firstIndex);
			result[1] = "insert " + scriptStr.substring(endIndex);
			scripts.add(result[0]);
			analysisMultipleScripts(result[1], scripts);
		}
	}

	public static List<String> filterScriptLine(BufferedReader reader) throws Exception {
		String analysisScript = firstFilterScript(reader).trim();
		analysisScript = analysisScript.substring(analysisScript.toLowerCase().indexOf("insert"));
		if (analysisScript.contains("$")) {
			analysisScript = sql$ReplaceTo0(analysisScript);
		}
		List<String> resultList = new ArrayList<>();
		analysisMultipleScripts(analysisScript, resultList);
		return resultList;
	}
	
	
	/**
	 * 过滤掉脚本的注释和一些不需要解析的脚本段
	 */
	public static String firstFilterScript(BufferedReader reader) throws Exception{
		StringBuilder script = new StringBuilder();
		Pattern pComments = Pattern.compile("(?ms)/\\*.*?\\*/|^\\s*//.*?$");
		String pPartition = "(alter)([\\s\\S]*?)(drop|add)([\\s\\S]*?)";
		try {
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (line.trim().startsWith("--") || line.trim().matches(pPartition)) {
					continue;
				}
				if (line.trim().contains("--")) {
					line = line.split("--")[0];
				}
				if (!(line.trim().startsWith("add"))) {
					script.append(line.trim() + "\n");
				}
			}
		} catch (IOException e) {
			throw e;
		}
		String analysisScript = pComments.matcher(script).replaceAll("").trim();
		return analysisScript;
	}
	
	public static String sql$ReplaceTo0(String sql) {
		String rel = "(\\$)(\\{).*?(\\})";
		Pattern p = Pattern.compile(rel, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
		Matcher m = p.matcher(sql);
		while (m.find()) {
			String str = m.group();
			sql = sql.replace(str, "0");
		}
		return sql;
	}
}
