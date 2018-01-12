package com.lsy.sparkproject.spark.session;

import org.apache.spark.AccumulatorParam;

import com.lsy.sparkproject.constant.Constants;
import com.lsy.sparkproject.util.StringUtils;

/**
 * session聚合统计Accumulator
 * 
 * @author work
 * 
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

	private static final long serialVersionUID = -1952556669867612387L;

	/**
	 * zero方法，用于数据初始化 返回所有范围区间的数量
	 */
	@Override
	public String zero(String v) {
		return Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s
				+ "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|" 
				+ Constants.STEP_PERIOD_1_3+ "=0|" 
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0";
	}

	/**
	 * addInPlace和addAccumulator可以理解为一样
	 * 主要实现，v1可能是初始化的那个连接串；v2就是遍历的时候判断出的某个session对应的区间
	 * ，在v1中找到v2对应的value，累加1，然后更新回连接串里去
	 */
	@Override
	public String addInPlace(String v1, String v2) {
		return add(v1, v2);
	}

	@Override
	public String addAccumulator(String v1, String v2) {
		return add(v1, v2);
	}

	/**
	 * session统计计算逻辑
	 * 
	 * @param v1
	 *            连接串
	 * @param v2
	 *            范围区间
	 * @return 更新以后的连接串
	 */
	private String add(String v1, String v2) {
		//检验v1是否为空
		if(StringUtils.isEmpty(v1)){
			return v2;
		}
		
		//从v1中提取v2对应的值，并累加1
		String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
		if(oldValue != null){
			int newValue = Integer.valueOf(oldValue) + 1;
			return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
		}
		
		return v1;

	}

}
