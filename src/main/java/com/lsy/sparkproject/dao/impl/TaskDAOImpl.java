package com.lsy.sparkproject.dao.impl;

import java.sql.ResultSet;

import com.lsy.sparkproject.dao.ITaskDAO;
import com.lsy.sparkproject.domain.Task;
import com.lsy.sparkproject.jdbc.JDBCHelper;

public class TaskDAOImpl implements ITaskDAO {

	public Task findById(long taskid) {
		final Task task = new Task();
		String sql = "select * from task where task_id=?";
		Object[] params = new Object[] { taskid };
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallback() {

			@Override
			public void process(ResultSet set) throws Exception {
				while (set.next()) {
					long taskid = set.getLong(1);
					String taskName = set.getString(2);
					String createTime = set.getString(3);
					String startTime = set.getString(4);
					String finishTime = set.getString(5);
					String taskType = set.getString(6);
					String taskStatus = set.getString(7);
					String taskParam = set.getString(8);
					task.setTaskid(taskid);
					task.setTaskName(taskName);
					task.setCreateTime(createTime);
					task.setStartTime(startTime);
					task.setFinishTime(finishTime);
					task.setTaskType(taskType);
					task.setTaskStatus(taskStatus);
					task.setTaskParam(taskParam);
				}
			}
		});
		return task;
	}

}
