package com.qianjiali2.hiveDependency.dataMap.utils;

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

import jline.internal.InputStreamReader;

public class HiveScriptFilterUtils2 {

	private static Set<String> opers = new HashSet<String>();

	private static List<String> list = new ArrayList<>();

	private static Pattern pComments = Pattern.compile("(?ms)/\\*.*?\\*/|^\\s*//.*?$");
	private static String pPartition = "(alter)([\\s\\S]*?)(drop|add)([\\s\\S]*?)";
	private static Pattern pinsert = Pattern.compile("(;)[\\s\\S]*?(insert)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	static {
		// param
		opers.add("; set");

		// DDL
		opers.add("; create");
		opers.add("; drop");
		opers.add("; alert");
		opers.add("; truncate");
		opers.add("; show");
		opers.add("; describe");

		// DML
		opers.add("; insert");
		opers.add("; load");
		opers.add("; update");
		opers.add("; delete");
		opers.add("; import");
		opers.add("; export");
		opers.add("; explain");

		// DQL
		opers.add("; select");
	}

	private static void excuteScriptOperToLowHandler(String wholeScript) {
		wholeScript = wholeScript.trim();
		wholeScript = wholeScript.replaceAll("; *[sS][eE][tT]", "; set");
		wholeScript = wholeScript.replaceAll("; *[sS][eE][lL][eE][cC][tT]", "; select");
		wholeScript = wholeScript.replaceAll("; *[iI][nN][sS][eE][rR][tT]", "; insert");
		wholeScript = wholeScript.replaceAll("; *[sS][hH][oO][wW]", "; show");

		wholeScript = wholeScript.replaceAll("; *[dD][eE][sS][cC][rR][iI][bB][eE]", "; describe");
		wholeScript = wholeScript.replaceAll("; *[eE][xX][pP][lL][aA][iI][nN]", "; explain");

		wholeScript = wholeScript.replaceAll("; *[cC][rR][eE][aA][tT][eE]", "; create");
		wholeScript = wholeScript.replaceAll("; *[dD][rR][oO][pP]", "; drop");
		wholeScript = wholeScript.replaceAll("; *[aA][lL][eE][rR][tT]", "; alert");
		wholeScript = wholeScript.replaceAll("; *[tT][rR][uU][nN][cC][aA][tT][eE]", "; truncate");

		wholeScript = wholeScript.replaceAll("; *[lL][oO][aA][dD]", "; load");
		wholeScript = wholeScript.replaceAll("; *[uU][pP][dD][aA][tT][eE]", "; update");
		wholeScript = wholeScript.replaceAll("; *[dD][eE][lL][eE][tT][eE]", "; delete");
		wholeScript = wholeScript.replaceAll("; *[iI][mM][pP][oO][rR][tT]", "; import");
		wholeScript = wholeScript.replaceAll("; *[eE][xX][pP][oO][rR][tT]", "; export");
	}

	public static List<String> readScriptContentOfFile(File file) throws IOException {
		BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		String content = "";
		StringBuilder sb = new StringBuilder();
		while (content != null) {
			content = bf.readLine();
			if (content == null) {
				break;
			}
			if (content.trim().startsWith("--") || content.trim().matches(pPartition)) {
				continue;
			}
			if (content.trim().contains("--")) {
				content = content.split("--")[0];
			}
			sb.append(content.trim() + " ");
		}
		String result = pComments.matcher(sb).replaceAll("").trim();
		bf.close();
		getScripts(result, list);
		return list;
	}
	
	public static void analysisMultipleScripts(String scriptStr, List<String> scripts) {
		Matcher m = pinsert.matcher(scriptStr.toLowerCase().trim());
		if (!m.find()) {
			if (scriptStr.trim().charAt(scriptStr.length() - 1) == ';') {// 避免最后一条执行语句的最后出现分号
				scriptStr = scriptStr.substring(0, scriptStr.length() - 1).trim();
			}
			scripts.add(scriptStr);
		} else {
			String str = m.group();
			int firstIndex = scriptStr.indexOf(str);
			int endIndex = firstIndex + str.length();
			String[] result = new String[2];
			result[0] = scriptStr.substring(0, firstIndex);
			result[1] = "insert "+scriptStr.substring(endIndex);
			scripts.add(result[0]);
			analysisMultipleScripts(result[1],scripts);
		}
	}

	public static List<String> filterScriptLine(BufferedReader reader) throws Exception {
		StringBuilder script = new StringBuilder();
		try {
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (line.trim().startsWith("--") || line.trim().matches(pPartition)) {
					continue;
				}
				if (line.trim().contains("--")) {
					line = line.split("--")[0];
				}
				script.append(line.trim() + "\n");
			}
		} catch (IOException e) {
			throw e;
		}

		String analysisScript = script.toString().trim();

		analysisScript = analysisScript.substring(analysisScript.indexOf("insert"));

		if (analysisScript.contains("$")) {
			analysisScript = sql$ReplaceTo0(analysisScript);
		}

		
		 List<String> resultList = new ArrayList<>();
		 
		 analysisMultipleScripts(analysisScript, resultList);
		 
		 return resultList;
	}

	private static void getScripts(String wholeScript, List<String> resultList) {

		excuteScriptOperToLowHandler(wholeScript);

		if (!isContainMuliScript(wholeScript)) {
			if (wholeScript.trim().charAt(wholeScript.length() - 1) == ';') {// 避免最后一条执行语句的最后出现分号
				wholeScript = wholeScript.substring(0, wholeScript.length() - 1).trim();
			}
			resultList.add(wholeScript);
			return;
		}

		if (wholeScript.toLowerCase().contains("; set")) {
			String[] wholeParts = splitHelper(wholeScript, "; set");
			wholeParts[1] = " set" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; select")) {
			String[] wholeParts = splitHelper(wholeScript, "; select");
			wholeParts[1] = " select" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; insert")) {
			String[] wholeParts = splitHelper(wholeScript, "; insert");
			wholeParts[1] = " insert" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; show")) {
			String[] wholeParts = splitHelper(wholeScript, "; show");
			wholeParts[1] = " show" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; describe")) {
			String[] wholeParts = splitHelper(wholeScript, "; describe");
			wholeParts[1] = " describe" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; explain")) {
			String[] wholeParts = splitHelper(wholeScript, "; explain");
			wholeParts[1] = " explain" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; create")) {
			String[] wholeParts = splitHelper(wholeScript, "; create");
			wholeParts[1] = " create" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; drop")) {
			String[] wholeParts = splitHelper(wholeScript, "; drop");
			wholeParts[1] = " drop" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; alert")) {
			String[] wholeParts = splitHelper(wholeScript, "; alert");
			wholeParts[1] = " alert" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; truncate")) {
			String[] wholeParts = splitHelper(wholeScript, "; truncate");
			wholeParts[1] = " truncate" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; load")) {
			String[] wholeParts = splitHelper(wholeScript, "; load");
			wholeParts[1] = " load" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; update")) {
			String[] wholeParts = splitHelper(wholeScript, "; update");
			wholeParts[1] = " update" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; delete")) {
			String[] wholeParts = splitHelper(wholeScript, "; delete");
			wholeParts[1] = " delete" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; import")) {
			String[] wholeParts = splitHelper(wholeScript, "; import");
			wholeParts[1] = " import" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		if (wholeScript.toLowerCase().contains("; export")) {
			String[] wholeParts = splitHelper(wholeScript, "; export");
			wholeParts[1] = " export" + wholeParts[1];
			wholeScript = wholeParts[0] + wholeParts[1];
			getScripts(wholeParts[0], resultList);
			wholeScript = wholeParts[1];
		}

		getScripts(wholeScript, resultList);
	}

	private static String[] splitHelper(String input, String splitContent) {
		String[] result = new String[2];
		if (input.toLowerCase().contains(splitContent)) {
			int firstIndex = input.toLowerCase().indexOf(splitContent.toLowerCase());
			int endIndex = firstIndex + splitContent.length();

			result[0] = input.substring(0, firstIndex);
			result[1] = input.substring(endIndex);

		}
		return result;
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

	/**
	 * 判断是否是多语句操作
	 * 
	 * @param wholeScript
	 * @return
	 */
	private static boolean isContainMuliScript(String wholeScript) {
		return opers.stream().filter(oper -> wholeScript.toLowerCase().contains(oper)).count() > 0;
	}
}
