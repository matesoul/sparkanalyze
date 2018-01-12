package com.lsy.sparkproject.spark.product;

import java.util.Arrays;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * 组内拼接去重函数
 * 
 * @author work
 *
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

	private static final long serialVersionUID = 1L;

	// 指定输入数据的字段与类型
	private StructType inputSchema = DataTypes
			.createStructType(Arrays.asList(DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));
	// 指定缓冲数组的字段与类型
	private StructType bufferSchema = DataTypes
			.createStructType(Arrays.asList(DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));
	// 指定返回类型
	private DataType datatype = DataTypes.StringType;
	// 指定是否是确定性的
	private boolean deterministic = true;

	@Override
	public StructType inputSchema() {
		return inputSchema;
	}

	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}

	@Override
	public DataType dataType() {
		return datatype;
	}

	@Override
	public boolean deterministic() {
		return deterministic;
	}

	/**
	 * 初始化，可以认为在内部自己指定一个初始值
	 */
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, "");
	}

	/**
	 * 更新，可以认为一个一个将组内的字段传递进来
	 */
	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		String bufferCityInfo = buffer.getString(0);
		String cityInfo = input.getString(0);
		// 去重
		if (!bufferCityInfo.contains(cityInfo)) {
			if ("".equals(bufferCityInfo)) {
				bufferCityInfo += cityInfo;
			} else {
				bufferCityInfo += "," + cityInfo;
			}
		}
	}

	/**
	 * 合并，各个节点上分布式拼接好的串合并起来
	 */
	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		String bufferCityInfo1 = buffer1.getString(0);
		String bufferCityInfo2 = buffer2.getString(0);

		for (String cityInfo : bufferCityInfo2.split(",")) {
			if (!bufferCityInfo1.contains(cityInfo)) {
				if ("".equals(bufferCityInfo1)) {
					bufferCityInfo1 += cityInfo;
				} else {
					bufferCityInfo1 += "," + cityInfo;
				}

			}
		}

		buffer1.update(0, bufferCityInfo1);
	}

	@Override
	public Object evaluate(Row row) {
		return row.getString(0);
	}

}
