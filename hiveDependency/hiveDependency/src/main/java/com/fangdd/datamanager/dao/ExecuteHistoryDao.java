package com.fangdd.datamanager.dao;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.fangdd.datamanager.DB.DBHelp;
import com.fangdd.datamanager.entity.QueryItem;
import com.fangdd.datamanager.entity.User;


public class ExecuteHistoryDao {
	
	public List<User> queryUserExecuteHistory(QueryItem queryItem) throws Exception{
		List<User> users = new ArrayList<User>();
		try {
			ResultSet result = DBHelp.excuteQuery(queryItem);
			while(result.next()){//hr.create_user_id,bu.`name`,bu.email,hr.excute_script,hr.create_time,bu.city_name
				User user = new User();
				user.setUsername(result.getString("user_name"));
				user.setEmail(result.getString("email"));
				user.setExcutescript(result.getString("excute_script"));
				user.setStarttime(result.getDate("create_time"));
				user.setCityname(result.getString("city_name"));
				users.add(user);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.toString());
		}
		return users;
	}
	
	private ExecuteHistoryDao() {}
	
	public static ExecuteHistoryDao getInstance(){
		return ExecuteHistoryDaoSingleton.EXECUTE_HISTORY_DAO;
	}
	private static class ExecuteHistoryDaoSingleton{
		public final static ExecuteHistoryDao EXECUTE_HISTORY_DAO = new ExecuteHistoryDao();
	}
}
