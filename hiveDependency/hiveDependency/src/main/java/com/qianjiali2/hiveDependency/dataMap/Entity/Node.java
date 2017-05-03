package com.qianjiali2.hiveDependency.dataMap.Entity;

public class Node {
	private String tablename;
	private Oper tableoper;

	public Node(String tablename, Oper tableoper) {
		super();
		this.tablename = tablename;
		this.tableoper = tableoper;
	}

	public String getTablename() {
		return tablename;
	}

	public Oper getTableoper() {
		return tableoper;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Node) {
			Node node = (Node) obj;
			return this.tablename.equals(node.getTablename());
		}
		return super.equals(obj);
	}
	
    public int hashCode() {
    	Node node = (Node) this;
        System.out.println("Hash" + node.tablename);
        return this.tablename.hashCode();
    }
}