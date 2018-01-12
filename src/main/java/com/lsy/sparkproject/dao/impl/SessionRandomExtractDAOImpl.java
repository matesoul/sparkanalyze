package com.lsy.sparkproject.dao.impl;

import com.lsy.sparkproject.dao.ISessionRandomExtractDAO;
import com.lsy.sparkproject.domain.SessionRandomExtract;
import com.lsy.sparkproject.jdbc.JDBCHelper;

/**
 * 随机抽取sessionDAO实现
 * @author work
 *
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {

	@Override
	public void insert(SessionRandomExtract sessionRandomExtract) {
		String sql = "insert into session_random_extract value(?,?,?,?,?)";
		Object[] params = new Object[]{
				sessionRandomExtract.getTaskid(),
				sessionRandomExtract.getSessionid(),
				sessionRandomExtract.getStartTime(),
				sessionRandomExtract.getSearchKeywords(),
				sessionRandomExtract.getClickCategoryIds()
		};
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
