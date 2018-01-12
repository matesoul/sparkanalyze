package com.lsy.sparkproject.dao;

import java.util.List;

import com.lsy.sparkproject.domain.SessionDetail;

/**
 * session明细DAO接口
 * @author work
 *
 */
public interface ISessionDetailDAO {
	void insert(SessionDetail sessionDetail);
	void insertBatch(List<SessionDetail> sessionDetails);
}
