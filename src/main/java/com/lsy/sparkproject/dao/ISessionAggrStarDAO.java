package com.lsy.sparkproject.dao;

import com.lsy.sparkproject.domain.SessionAggrStat;

/**
 * session聚合统计模块DAO接口
 * 
 * @author work
 * 
 */
public interface ISessionAggrStarDAO {
	/**
	 * 插入session聚合统计结果
	 * @param sessionAggrStat
	 */
	void insert(SessionAggrStat sessionAggrStat);
}
