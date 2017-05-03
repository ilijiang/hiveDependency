package com.fangdd.datamanager.serviceImpl;

import java.util.List;

import com.fangdd.datamanager.dao.ExecuteHistoryDao;
import com.fangdd.datamanager.entity.QueryItem;
import com.fangdd.datamanager.entity.User;
import com.fangdd.datamanager.service.IExecuteHistoryService;

public class ExecuteHistoryServiceImpl implements IExecuteHistoryService {
	
	private ExecuteHistoryDao executeHistoryDao =  ExecuteHistoryDao.getInstance();

	@Override
	public List<User> getUserExcuteHistory(String scriptStr, String startTime) throws Exception {
		QueryItem queryItem = new QueryItem(scriptStr, startTime);
		return executeHistoryDao.queryUserExecuteHistory(queryItem);
	}

	@Override
	public List<User> getUserExcuteHistoryByscriptStr(String scriptStr) throws Exception {
		QueryItem queryItem = new QueryItem();
		queryItem.setScript(scriptStr);
		return executeHistoryDao.queryUserExecuteHistory(queryItem);
	}

	@Override
	public List<User> getUserExcuteHistoryBystartTime(String startTime) throws Exception {
		QueryItem queryItem = new QueryItem();
		queryItem.setStarttime(startTime);
		return executeHistoryDao.queryUserExecuteHistory(queryItem);
	}

	//private ExecuteHistoryServiceImpl(){}
	
}
