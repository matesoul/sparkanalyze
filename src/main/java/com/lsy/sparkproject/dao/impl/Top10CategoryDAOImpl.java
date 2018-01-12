package com.lsy.sparkproject.dao.impl;

import com.lsy.sparkproject.dao.ITop10CategoryDAO;
import com.lsy.sparkproject.domain.Top10Category;
import com.lsy.sparkproject.jdbc.JDBCHelper;

/**
 * top10品类DAO实现
 * @author work
 *
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO{

	@Override
	public void insert(Top10Category category) {
		String sql = "insert into top10_category value(?,?,?,?,?)";
		Object[] params = new Object[]{
				category.getTaskid(),
				category.getCategoryid(),
				category.getClickcount(),
				category.getOrdercount(),
				category.getPaycount()
		};
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
