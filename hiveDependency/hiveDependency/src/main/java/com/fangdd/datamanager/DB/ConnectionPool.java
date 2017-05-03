package com.fangdd.datamanager.DB;

import java.sql.Connection;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConnectionPool {
	
	private static int connCount = 2;
	
    private static ConcurrentLinkedQueue<Connection> connPool = new ConcurrentLinkedQueue<Connection>();
    
    static{
    	for(int i=0;i<connCount;i++){
    		try {
				connPool.offer(DBHelp.createHiveConnection());
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("db connection create failed is:"+e.toString());
			}
    	}
    }
    
    public static Connection getDBConnection(){
    	return connPool.poll();
    }
    
    public static void closeDBConnection(Connection conn){
    	connPool.offer(conn);
    }
}
