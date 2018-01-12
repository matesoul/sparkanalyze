package com.lsy.sparkproject.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.alibaba.fastjson.JSONObject;
import com.lsy.sparkproject.conf.ConfigurationManager;
import com.lsy.sparkproject.constant.Constants;
import com.lsy.sparkproject.test.MockData;

/**
 * spark工具类
 * 
 * @author work
 * 
 */
public class SparkUtils {
	public static void setMaster(SparkConf conf) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			conf.setMaster("local");
		}
	}

	/**
	 * 生成模拟数据
	 * 
	 * @param sc
	 * @param sqlContext
	 */
	public static void mackData(JavaSparkContext sc, SQLContext sqlContext) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			MockData.mock(sc, sqlContext);
		}
	}
	
	/**
	 * 获取SQLContext
	 * @param sc
	 * @return
	 */
	public static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}
	
	/**
	 * 获取指定日期范围内的行为数据行为的RDD
	 * @param sqlContext
	 * @param taskParam
	 * @return
	 */
	public static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext,
			JSONObject taskParam) {
		String startDate = ParamUtils.getParam(taskParam,
				Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam,
				Constants.PARAM_END_DATE);
		String sql = "select *" + "from user_visit_action " + "where date>='"
				+ startDate + "'" + "and date<='" + endDate + "'";
		DataFrame actionDF = sqlContext.sql(sql);
		// return actionDF.javaRDD().repartition(1000);
		return actionDF.javaRDD();
	}

}
