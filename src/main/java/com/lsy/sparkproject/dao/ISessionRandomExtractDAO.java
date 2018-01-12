package com.lsy.sparkproject.dao;

import com.lsy.sparkproject.domain.SessionRandomExtract;

/**
 * session随机抽取模块DAO接口
 * 
 * @author work
 * 
 */
public interface ISessionRandomExtractDAO {
	/**
	 * 插入session随机抽取
	 * @param sessionAggrStat
	 */
	void insert(SessionRandomExtract sessionRandomExtract);
}
