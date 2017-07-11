package com.qianjiali.hiveDependency.entity;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

public class ParseNode {
	private Set<String> tableTargetObjects = new HashSet<String>();
	private Set<String> tableSourceObjects = new HashSet<String>();
	private Stack<String> tableNameStack = new Stack<String>();

	public Set<String> getTableTargetObjects() {
		return tableTargetObjects;
	}

	public void setTableTargetObjects(Set<String> tableTargetObjects) {
		this.tableTargetObjects = tableTargetObjects;
	}

	public Set<String> getTableSourceObjects() {
		return tableSourceObjects;
	}

	public void setTableSourceObjects(Set<String> tableSourceObjects) {
		this.tableSourceObjects = tableSourceObjects;
	}

	public Stack<String> getTableNameStack() {
		return tableNameStack;
	}

	public void setTableNameStack(Stack<String> tableNameStack) {
		this.tableNameStack = tableNameStack;
	}
}
