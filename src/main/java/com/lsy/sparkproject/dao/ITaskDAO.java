package com.lsy.sparkproject.dao;

import com.lsy.sparkproject.domain.Task;

/**
 * 任务管理DAO接口
 * @author work
 *
 */
public interface ITaskDAO {
	/**
	 * 根据主键查询任务
	 */
	Task findById(long taskid);
}
