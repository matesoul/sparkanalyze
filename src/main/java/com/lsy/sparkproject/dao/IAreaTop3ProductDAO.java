package com.lsy.sparkproject.dao;

import java.util.List;

import com.lsy.sparkproject.domain.AreaTop3Product;

public interface IAreaTop3ProductDAO {
	void insertBatch(List<AreaTop3Product> areaTop3Products);
}
