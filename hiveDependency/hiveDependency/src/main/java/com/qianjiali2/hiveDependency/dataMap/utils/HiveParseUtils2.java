package com.qianjiali2.hiveDependency.dataMap.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPrivateKeySpec;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import com.jcraft.jsch.Cipher;
import com.qianjiali2.hiveDependency.dataMap.Entity.Node;
import com.qianjiali2.hiveDependency.dataMap.Entity.Oper;
import com.qianjiali2.hiveDependency.dataMap.Entity.ParseNode2;

/**
 * 目的：获取AST中的表，列，以及对其所做的操作，如SELECT,INSERT 重点：获取SELECT操作中的表和列的相关操作。其他操作这判断到表级别。
 * 实现思路：对AST深度优先遍历，遇到操作的token则判断当前的操作，
 * 遇到TOK_TAB或TOK_TABREF则判断出当前操作的表，遇到子句则压栈当前处理，处理子句。 子句处理完，栈弹出。
 *
 */
public class HiveParseUtils2 {

	////////////////////////////
	private ParseNode2 parseNode2;

	/////////////////////////////

	public HiveParseUtils2(ParseNode2 parseNode2) {
		this.parseNode2 = parseNode2;
	}

	private static final String UNKNOWN = "UNKNOWN";
	private Stack<String> tableNameStack = new Stack<String>();
	private Stack<Oper> operStack = new Stack<Oper>();

	private String nowQueryTable = "";// 定义及处理不清晰，修改为query或from节点对应的table集合或许好点。目前正在查询处理的表可能不止一个。

	private Oper oper;
	private boolean joinClause = false;

	/**
	 * 对一个节点进行一整套流程的操作：但是这个节点优先从叶子节点出发
	 * 
	 * @param ast
	 * @return
	 */
	public Set<String> parseIteral(ASTNode ast) {
		Set<String> set = new HashSet<String>();// 当前查询所对应到的表集合
		prepareToParseCurrentNodeAndChilds(ast);
		set.addAll(parseChildNodes(ast));
		set.addAll(parseCurrentNode(ast, set));
		endParseCurrentNode(ast);
		return set;
	}

	private void endParseCurrentNode(ASTNode ast) {
		if (ast.getToken() != null) {
			switch (ast.getToken().getType()) {// join 从句结束，跳出join
			case HiveParser.TOK_RIGHTOUTERJOIN:
			case HiveParser.TOK_LEFTOUTERJOIN:
			case HiveParser.TOK_JOIN:
				joinClause = false;
				break;
			case HiveParser.TOK_QUERY:
				// break;
			case HiveParser.TOK_INSERT:
			case HiveParser.TOK_CREATETABLE:
			case HiveParser.TOK_SELECT:
				nowQueryTable = tableNameStack.pop();
				oper = operStack.pop();
				break;
			}
		}
	}

	private Set<String> parseCurrentNode(ASTNode ast, Set<String> set) {
		if (ast.getToken() != null) {
			switch (ast.getToken().getType()) {
			case HiveParser.TOK_TABLE_PARTITION:
			case HiveParser.TOK_TABNAME:
				if (oper == Oper.CREATETABLE) {
					if (ast.getChildCount() == 2) {
						String dbname = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
						String table = dbname + "." + BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
						if (oper == Oper.SELECT) {
							nowQueryTable = table;
						}
						parseNode2.setTableTargetObjects(new Node(table, oper));
					}
				}
				break;
			case HiveParser.TOK_TAB:// outputTable target_table
				String tableTab = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
				if (oper == Oper.SELECT) {
					nowQueryTable = tableTab;
				}
				parseNode2.setTableTargetObjects(new Node(tableTab, oper));
				break;
			case HiveParser.TOK_TABREF:// inputTable source_table
				ASTNode tabTree = (ASTNode) ast.getChild(0);
				String tableName = (tabTree.getChildCount() == 1)
						? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
						: BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "."
								+ tabTree.getChild(1);
				if (oper == Oper.SELECT) {
					if (joinClause && !"".equals(nowQueryTable)) {
						nowQueryTable += "&" + tableName;//
					} else {
						nowQueryTable = tableName;
					}
					set.add(tableName);
				}
				parseNode2.setTableSourceObjects(new Node(tableName, oper));
				break;

			}
		}
		return set;
	}

	/**
	 * 一直遍历到叶子节点，并对次叶子节点进行操作
	 * 
	 * @param ast
	 * @return
	 */
	private Set<String> parseChildNodes(ASTNode ast) {
		Set<String> set = new HashSet<String>();
		int numCh = ast.getChildCount();
		if (numCh > 0) {
			for (int num = 0; num < numCh; num++) {
				ASTNode child = (ASTNode) ast.getChild(num);
				set.addAll(parseIteral(child));
			}
		}
		return set;
	}

	/**
	 * 解析当前的操作，并对相应操作入栈
	 * 
	 * @param ast
	 */
	private void prepareToParseCurrentNodeAndChilds(ASTNode ast) {
		if (ast.getToken() != null) {
			switch (ast.getToken().getType()) {// join 从句开始
			case HiveParser.TOK_RIGHTOUTERJOIN:
			case HiveParser.TOK_LEFTOUTERJOIN:
			case HiveParser.TOK_JOIN:
				joinClause = true;
				break;
			case HiveParser.TOK_QUERY:
				tableNameStack.push(nowQueryTable);
				operStack.push(oper);
				nowQueryTable = "";// sql22
				oper = Oper.SELECT;
				break;
			case HiveParser.TOK_INSERT:
				tableNameStack.push(nowQueryTable);
				operStack.push(oper);
				oper = Oper.INSERT;
				break;
			case HiveParser.TOK_SELECT:
				tableNameStack.push(nowQueryTable);
				operStack.push(oper);
				oper = Oper.SELECT;
				break;
			case HiveParser.TOK_CREATETABLE:
				tableNameStack.push(nowQueryTable);
				operStack.push(oper);
				oper = Oper.CREATETABLE;
				break;
			}
		}
	}

	public static String unescapeIdentifier(String val) {
		if (val == null) {
			return null;
		}
		if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
			val = val.substring(1, val.length() - 1);
		}
		return val;
	}


	public void parse(ASTNode ast) {
		parseIteral(ast);
	}

	public static void main(String[] args) throws Exception {
		
		
		ParseDriver pd = new ParseDriver();
        String parsesql = getScriptBylocal();
        //System.out.println(parsesql);
        ASTNode ast = pd.parse(parsesql);
		//System.out.println(ast.toStringTree());
		
		
		ParseNode2 pNode2 = new ParseNode2();
		HiveParseUtils2 hp = new HiveParseUtils2(pNode2);
		hp.parse(ast);
		demo(pNode2);
	}
	
	

	public static void demo(ParseNode2 pNode2) {
		
		
		System.out.println("\n\nsource table=======>>>>>");
		for (Node node : pNode2.getTableSourceObjects()) {
			System.out.println(node.getTablename() + "==>>" + node.getTableoper());
		}
		System.out.println("target table=======>>>>>");
		for (Node node : pNode2.getTableTargetObjects()) {
			System.out.println(node.getTablename() + "==>>" + node.getTableoper());
		}
	}
	
	
	public static String getScriptBylocal() {
		BufferedReader reader = null;
		StringBuilder builder = new StringBuilder();
		try {
			String line;
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(new File("C:\\Users\\sh-yanggang\\Desktop\\HiveParseSQL\\testScript.txt"))
					, "UTF-8"));
			while ((line = reader.readLine()) != null) {
				builder.append(line + "\n");
			}
		} catch (Exception e) {
			try {
				reader.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		return builder.toString();
	}

}