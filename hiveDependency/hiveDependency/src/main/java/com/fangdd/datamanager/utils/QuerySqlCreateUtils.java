package com.fangdd.datamanager.utils;

import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import com.fangdd.datamanager.entity.QueryItem;

public class QuerySqlCreateUtils {

	private static String commonSql = "select hr.create_user_id,du.user_name,bu.email,hr.excute_script,hr.create_time,bu.city_name\n"
			+" from hive_recent_query hr\n"
			+" INNER JOIN data_users du on du.id = hr.create_user_id\n"
			+" LEFT JOIN bi_user bu on du.user_name = bu.ename";
	
	private static String scriptQuerySql = commonSql+" where hr.excute_script like ''{0}%''";
	
	private static String startTimeQuerySql = commonSql+" where hr.create_time like '{0}%'";
	
	private static String  querySql = commonSql+" where hr.excute_script like ''{0}%'' and hr.create_time like ''{1}%''";
	
	public static String getQuerySql(QueryItem item) throws Exception{
		String sql = "";
		List<String> values = new ArrayList<String>();
		Class<?> clazz = item.getClass();
		Field[] fields = clazz.getDeclaredFields();
		for(Field field:fields){
			field.setAccessible(true);
			String value = String.valueOf(field.get(item)).trim();
			if(value!=null &&(!value.equals("null")) && value.length()>0){
				String fieldname = field.getName();
				System.out.println("属性（"+fieldname+"）的值是====>>>>>>"+value);
				if(fieldname.equals("script")){
					sql = MessageFormat.format(scriptQuerySql, value);
					values.add(value);
				}else if(fieldname.equals("starttime")){
					sql = MessageFormat.format(startTimeQuerySql, value);
					values.add(value);
				}
			}
		}
		if(values.size()==2){
			sql = MessageFormat.format(querySql,values.get(0),values.get(1));
		}
		System.out.println("执行的历史查询语句是===》》"+sql);
		return sql;
	}
}
