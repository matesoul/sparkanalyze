package com.lsy.sparkproject.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.lsy.sparkproject.dao.IAreaTop3ProductDAO;
import com.lsy.sparkproject.domain.AreaTop3Product;
import com.lsy.sparkproject.jdbc.JDBCHelper;

public class AreaTop3ProductDAOImpl implements IAreaTop3ProductDAO{

	@Override
	public void insertBatch(List<AreaTop3Product> areaTop3Products) {
			String sql = "INSERT INTO area_top3_roduct VALUES(?,?,?,?,?,?,?,?)";
			List<Object[]> paramList = new ArrayList<Object[]>();
			
			for(AreaTop3Product areaTop3Product : areaTop3Products){
				Object[] params = new Object[8];
				params[0] = areaTop3Product.getTaskid();
				params[1] = areaTop3Product.getArea();
				params[2] = areaTop3Product.getAreaLevel();
				params[3] = areaTop3Product.getProductid();
				params[4] = areaTop3Product.getCityInfos();
				params[5] = areaTop3Product.getClickCount();
				params[6] = areaTop3Product.getProductName();
				params[7] = areaTop3Product.getProductStatus();
				
				paramList.add(params);
			}
			
			JDBCHelper jdbcHelper = JDBCHelper.getInstance();
			jdbcHelper.executeBatch(sql, paramList);
	}

}
