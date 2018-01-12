package com.lsy.sparkproject.dao.factory;

import com.lsy.sparkproject.dao.IAreaTop3ProductDAO;
import com.lsy.sparkproject.dao.IPageSplitConvertRateDAO;
import com.lsy.sparkproject.dao.ISessionAggrStarDAO;
import com.lsy.sparkproject.dao.ISessionDetailDAO;
import com.lsy.sparkproject.dao.ISessionRandomExtractDAO;
import com.lsy.sparkproject.dao.ITaskDAO;
import com.lsy.sparkproject.dao.ITop10CategoryDAO;
import com.lsy.sparkproject.dao.ITop10SessionDAO;
import com.lsy.sparkproject.dao.impl.AreaTop3ProductDAOImpl;
import com.lsy.sparkproject.dao.impl.PageSplitConvertRateDAOImpl;
import com.lsy.sparkproject.dao.impl.SessionAggrStatDAOImpl;
import com.lsy.sparkproject.dao.impl.SessionDetailDAOImpl;
import com.lsy.sparkproject.dao.impl.SessionRandomExtractDAOImpl;
import com.lsy.sparkproject.dao.impl.TaskDAOImpl;
import com.lsy.sparkproject.dao.impl.Top10CategoryDAOImpl;
import com.lsy.sparkproject.dao.impl.Top10SessionDAOImpl;

/**
 * DAO工厂类
 * @author work
 *
 */
public class DAOFactory {
	/**
	 * 获取任务管理DAO
	 * @return DAO
	 */
	public static ITaskDAO getTaskDAO(){
		return new TaskDAOImpl();
	}
	
	public static ISessionAggrStarDAO getSessionAggrStatDAO(){
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
		return new SessionRandomExtractDAOImpl();
	}
	
	public static ISessionDetailDAO getSessionDetailDAO(){
		return new SessionDetailDAOImpl();
	}
	public static ITop10CategoryDAO getTop10CategoryDAO(){
		return new Top10CategoryDAOImpl();
	}
	public static ITop10SessionDAO getTop10SessionDAO(){
		return new Top10SessionDAOImpl();
	}
	public static IPageSplitConvertRateDAO getPageSlitConvertRateDAO(){
		return new PageSplitConvertRateDAOImpl();
	}
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO(){
		return new AreaTop3ProductDAOImpl();
	}
}
