package com.qianjiali2.hiveDependency.dataMap.Entity;

import java.util.HashSet;
import java.util.Set;

public class ParseNode2 {
	private Set<Node> tableTargetObjects = new HashSet<Node>();
	private Set<Node> tableSourceObjects = new HashSet<Node>();

	public Set<Node> getTableTargetObjects() {
		return tableTargetObjects;
	}

	public void setTableTargetObjects(Node node) {
		this.tableTargetObjects.add(node);
	}

	public Set<Node> getTableSourceObjects() {
		return tableSourceObjects;
	}

	public void setTableSourceObjects(Node node) {
		this.tableSourceObjects.add(node);
	}
}
