package com.fangdd.datamanager.service;

import java.util.List;

import com.fangdd.datamanager.entity.QueryItem;
import com.fangdd.datamanager.entity.User;


public interface IExecuteHistoryService {
    public List<User> getUserExcuteHistory(String scriptStr,String startTime) throws Exception;
    public List<User> getUserExcuteHistoryByscriptStr(String scriptStr) throws Exception;
    public List<User> getUserExcuteHistoryBystartTime(String startTime) throws Exception;
}
