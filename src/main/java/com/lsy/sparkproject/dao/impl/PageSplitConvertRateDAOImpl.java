package com.lsy.sparkproject.dao.impl;

import com.lsy.sparkproject.dao.IPageSplitConvertRateDAO;
import com.lsy.sparkproject.domain.PageSplitConvertRate;
import com.lsy.sparkproject.jdbc.JDBCHelper;

/**
 * 页面切片转化率DAO实现类
 * 
 * @author work
 * 
 */
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO{

	@Override
	public void insert(PageSplitConvertRate pageSplitConvertRate) {
		String sql = "insert into page_split_convert_rate value(?,?)";
		Object[] params = new Object[]{
				pageSplitConvertRate.getTaskid(),
				pageSplitConvertRate.getConvertRate()
		};
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
