package com.qianjiali2.hiveDependency.dataMap.utils;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

import com.qianjiali.hiveDependency.entity.ParseNode;

/**
 * 目的：获取AST中的表，列，以及对其所做的操作，如SELECT,INSERT 重点：获取SELECT操作中的表和列的相关操作。其他操作这判断到表级别。
 * 实现思路：对AST深度优先遍历，遇到操作的token则判断当前的操作，
 * 遇到TOK_TAB或TOK_TABREF则判断出当前操作的表，遇到子句则压栈当前处理，处理子句。 子句处理完，栈弹出。
 *
 */
public class HiveParseUtils {

	private ParseNode parseNode;
	private String scriptStr;

	public HiveParseUtils(ParseNode parseNode, String scriptStr) {
		super();
		this.parseNode = parseNode;
		this.scriptStr = scriptStr;
	}

	private static int JoinSequence = 0;

	private JoinType joinType;

	private boolean joinClause = false;
	private Oper oper;

	private enum Oper {
		SELECT, INSERT, DROP, TRUNCATE, LOAD, CREATETABLE, ALTER, DELETE, RIGHTOUTJOIN, LEFTOUTJOIN, INNERJOIN
	}

	private enum JoinType {
		RIGHTOUTJOIN, LEFTOUTJOIN, INNERJOIN
	}

	private void parseChildNodes(ASTNode ast) {
		int numCh = ast.getChildCount();
		if (numCh > 0) {
			for (int num = 0; num < numCh; num++) {
				ASTNode child = (ASTNode) ast.getChild(num);
				if (ast.getToken() != null) {
					operateParse(ast);
					tableParse(ast);
				}
				parseChildNodes(child);
				endOperateParse(ast);
			}
		}
	}

	private void operateParse(ASTNode ast) {
		switch (ast.getToken().getType()) {
		case HiveParser.TOK_RIGHTOUTERJOIN:
			joinClause = true;
			joinType = JoinType.RIGHTOUTJOIN;
			break;
		case HiveParser.TOK_LEFTOUTERJOIN:
			joinClause = true;
			joinType = JoinType.LEFTOUTJOIN;
			break;
		case HiveParser.TOK_JOIN:
			joinClause = true;
			joinType = JoinType.INNERJOIN;
			break;
		case HiveParser.TOK_QUERY:
			oper = Oper.SELECT;
			break;
		case HiveParser.TOK_INSERT:
			oper = Oper.INSERT;
			break;
		case HiveParser.TOK_SELECT:
			oper = Oper.SELECT;
			break;
		case HiveParser.TOK_DROPTABLE:
			oper = Oper.DROP;
			break;
		case HiveParser.TOK_TRUNCATETABLE:
			oper = Oper.TRUNCATE;
			break;
		case HiveParser.TOK_LOAD:
			oper = Oper.LOAD;
			break;
		case HiveParser.TOK_CREATETABLE:
			oper = Oper.CREATETABLE;
			break;
		case HiveParser.TOK_DELETE_FROM:
			oper = Oper.DELETE;
			break;
		}
	}

	private void tableParse(ASTNode ast) {
		switch (ast.getToken().getType()) {
		case HiveParser.TOK_TABLE_PARTITION:
			if (ast.getChildCount() != 2) {
				String table = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
				parseNode.getTableTargetObjects().add(table + "\t" + oper);
			}
			break;
		case HiveParser.TOK_TAB:// outputTable
			String tableTab = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
			parseNode.getTableTargetObjects().add(tableTab + "\t" + oper);
			break;
		case HiveParser.TOK_TABREF:// inputTable
			ASTNode tabTree = (ASTNode) ast.getChild(0);
			String tableName = (tabTree.getChildCount() == 1)
					? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
					: BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree.getChild(1);
			if (joinClause && JoinSequence != 0) {
				parseNode.getTableNameStack().push(tableName + "\t" + oper);
			} else {
				parseNode.getTableSourceObjects().add(tableName + "\t" + oper);
			}
			JoinSequence++;
			break;
		case HiveParser.TOK_TABNAME:// outputTable
			String tableNameDel = ast.getChild(0).getText().toLowerCase();
			if (oper == Oper.DELETE || oper == Oper.CREATETABLE || oper == Oper.DROP) {
				parseNode.getTableTargetObjects().add(tableNameDel + "\t" + oper);
			}
			break;
		}
	}

	private void endOperateParse(ASTNode ast) {
		if (ast.getToken() != null) {
			switch (ast.getToken().getType()) {
			case HiveParser.TOK_RIGHTOUTERJOIN:
			case HiveParser.TOK_LEFTOUTERJOIN:
			case HiveParser.TOK_JOIN:
				joinClause = false;
				for (int i = 0; i < parseNode.getTableNameStack().size(); i++) {
					parseNode.getTableSourceObjects().add(parseNode.getTableNameStack().pop());
				}
				break;
			}
		}
	}

	private String unescapeIdentifier(String val) {
		if (val == null) {
			return null;
		}
		if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
			val = val.substring(1, val.length() - 1);
		}
		return val;
	}

	private ASTNode parseScript2ASTNode() throws Exception {
		try {
			ParseDriver pd = new ParseDriver();
			ASTNode ast = pd.parse(scriptStr);
			return ast;
		} catch (ParseException e) {
			throw e;
		}
	}

	public ParseNode parse() throws Exception {
		try {
			parseChildNodes(parseScript2ASTNode());
		} catch (Exception e) {
			throw e;
		}
		return parseNode;
	}
}
