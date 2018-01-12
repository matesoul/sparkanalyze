package com.lsy.sparkproject.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.lsy.sparkproject.dao.ISessionDetailDAO;
import com.lsy.sparkproject.domain.SessionDetail;
import com.lsy.sparkproject.jdbc.JDBCHelper;

/**
 * session明细DAO接口
 * 
 * @author work
 * 
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO {

	@Override
	public void insert(SessionDetail sessionDetail) {
		String sql = "insert into session_detail value(?,?,?,?,?,?,?,?,?,?,?,?)";
		Object[] params = new Object[] { sessionDetail.getTaskid(),
				sessionDetail.getUserid(), sessionDetail.getSessionid(),
				sessionDetail.getPageid(), sessionDetail.getActionTime(),
				sessionDetail.getSearchKeyword(),
				sessionDetail.getClickCategoryId(),
				sessionDetail.getClickProductId(),
				sessionDetail.getOrderCategoryIds(),
				sessionDetail.getOrderProductIds(),
				sessionDetail.getPayCategoryIds(),
				sessionDetail.getPayProductIds() };
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

	@Override
	public void insertBatch(List<SessionDetail> sessionDetails) {
		String sql = "insert into session_detail value(?,?,?,?,?,?,?,?,?,?,?,?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		for (SessionDetail sessionDetail : sessionDetails) {
			Object[] params = new Object[] { sessionDetail.getTaskid(),
					sessionDetail.getUserid(), sessionDetail.getSessionid(),
					sessionDetail.getPageid(), sessionDetail.getActionTime(),
					sessionDetail.getSearchKeyword(),
					sessionDetail.getClickCategoryId(),
					sessionDetail.getClickProductId(),
					sessionDetail.getOrderCategoryIds(),
					sessionDetail.getOrderProductIds(),
					sessionDetail.getPayCategoryIds(),
					sessionDetail.getPayProductIds() };
			paramsList.add(params);
		}

		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, paramsList);

	}

}
