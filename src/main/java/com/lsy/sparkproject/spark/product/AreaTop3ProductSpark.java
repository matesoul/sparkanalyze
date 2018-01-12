package com.lsy.sparkproject.spark.product;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;
import com.lsy.sparkproject.conf.ConfigurationManager;
import com.lsy.sparkproject.constant.Constants;
import com.lsy.sparkproject.dao.IAreaTop3ProductDAO;
import com.lsy.sparkproject.dao.ITaskDAO;
import com.lsy.sparkproject.dao.factory.DAOFactory;
import com.lsy.sparkproject.domain.AreaTop3Product;
import com.lsy.sparkproject.domain.Task;
import com.lsy.sparkproject.util.ParamUtils;
import com.lsy.sparkproject.util.SparkUtils;

import scala.Tuple2;

/**
 * 各区域top3热门商品统计spark作业
 * 
 * @author work
 *
 */
public class AreaTop3ProductSpark {
	public static void main(String[] args) {
		// 创建Sparkonf
		SparkConf conf = new SparkConf().setAppName("AreaTop3ProductSpark");
		SparkUtils.setMaster(conf);

		// 构建Saprk上下文
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

		// 注册自定义函数
		sqlContext.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
		sqlContext.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());
		sqlContext.udf().register("get_json_object", new GetJsonObjectUDF(), DataTypes.StringType);

		// 准备模拟数据
		SparkUtils.mackData(sc, sqlContext);

		// 获取命令行传入的taskid，查询对应的任务参数
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
		Task task = taskDAO.findById(taskid);
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		String startDate = ParamUtils.getParam(taskParam, Constants.FIELD_START_TIME);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

		// 查询用户指定日期范围内的点击行为数据
		JavaPairRDD<Long, Row> Cityid2ClickActionRDD = getCityid2ClickActionRDDByDate(sqlContext, startDate, endDate);

		// 从MySQL中查询城市信息s
		JavaPairRDD<Long, Row> Cityid2CityInfoRDD = getCityid2CityInfoRDD(sqlContext);

		// 生成点击商品基础信息临时表
		generateClickProductBasicTable(sqlContext, Cityid2ClickActionRDD, Cityid2CityInfoRDD);

		// 生成各区域商品点次数的临时表
		generateTmpAreaProductClickCount(sqlContext);
		
		//生成包含完整商品信息的各区域各商品点击次数的临时表
		generateTmpAreaFullProductClickCountTable(sqlContext);
		
		//使用开窗函数获取各个区域内点击次数排名前3的热门商品
		JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);
		
		//写入MySQL,数据量少，可以直接写入
		List<Row> rows = areaTop3ProductRDD.collect();
		persistAreaTop3Product(taskid, rows);

		sc.close();
	}

	/**
	 * 查询指定日期范围的点击行为数据
	 * 
	 * @param startDate
	 * @param endDate
	 * @return 点击行为数据
	 */
	private static JavaPairRDD<Long, Row> getCityid2ClickActionRDDByDate(SQLContext sqlContext, String startDate,
			String endDate) {
		String sql = "SELECT city_id,click_product_id product_id " + "WHERE click_product_id IS NOT NULL"
				 + "AND date>='"
				+ startDate + "'" + "AND date<='" + endDate + "'";

		DataFrame clickActionDf = sqlContext.sql(sql);

		JavaRDD<Row> clickActionRDD = clickActionDf.javaRDD();
		JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(new PairFunction<Row, Long, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Row> call(Row t) throws Exception {
				Long cityid = t.getLong(0);
				return new Tuple2<Long, Row>(cityid, t);
			}
		});
		return cityid2clickActionRDD;
	}

	private static JavaPairRDD<Long, Row> getCityid2CityInfoRDD(SQLContext sqlContext) {
		// 构建Mysql连接配置信息
		String url = null;
		String user = null;
		String password = null;
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

		if (local) {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER);
			password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
		} else {
			url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
			user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
			password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
		}

		Map<String, String> options = new HashMap<String, String>();
		options.put("url", url);
		options.put("dbtable", "city_info");
		options.put("user", user);
		options.put("password", password);

		// 通过SQLContext去从Mysql查询数据
		DataFrame cityInfoDF = sqlContext.read().format("jdbc").options(options).load();

		JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();
		JavaPairRDD<Long, Row> cityid2cityInfoRDD = cityInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Row> call(Row t) throws Exception {
				Long cityid = Long.valueOf(String.valueOf(t.get(0)));
				return new Tuple2<Long, Row>(cityid, t);
			}
		});
		return cityid2cityInfoRDD;
	}

	/**
	 * 关联点击行为数据与城市信息数据
	 * 
	 * @param Cityid2ClickActionRDD
	 * @param Cityid2CityInfoRDD
	 */
	private static void generateClickProductBasicTable(SQLContext sqlContext,
			JavaPairRDD<Long, Row> Cityid2ClickActionRDD, JavaPairRDD<Long, Row> Cityid2CityInfoRDD) {
		JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = Cityid2ClickActionRDD.join(Cityid2CityInfoRDD);

		// 将上面的JavaPairRDD转换成JavaRDD<Row>（才能将RDD转换成DataFrame）
		JavaRDD<Row> mappedRdd = joinedRDD.map(new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<Long, Tuple2<Row, Row>> v1) throws Exception {
				long cityid = v1._1;
				Row clickAction = v1._2._1;
				Row cityInfo = v1._2._2;

				long productid = clickAction.getLong(1);
				String cityName = cityInfo.getString(1);
				String area = cityInfo.getString(2);
				return RowFactory.create(cityid, cityName, area, productid);
			}
		});

		// 转换为DataFrame
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
		structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));

		StructType schema = DataTypes.createStructType(structFields);

		DataFrame df = sqlContext.createDataFrame(mappedRdd, schema);

		// 将DataFrame中的数据注册成临时表（tmp_click_product_basic）
		df.registerTempTable("tmp_click_product_basic");
	}

	/**
	 * 生成各区域商品点击次数临时表
	 * 
	 * @param sqlContext
	 */
	private static void generateTmpAreaProductClickCount(SQLContext sqlContext) {
		String sql = "SELECT area,product_id,count(*) click_count,group_concat_distinct(concat_long_string(cityid,city_name,':')) city_infos"
				+ "FROM tmp_click_product_basic " + "GROUP BY area,product_id";
		DataFrame df = sqlContext.sql(sql);

		// 将查询的数据注册为一个临时表
		df.registerTempTable("tmp_area_product_click_count");
	}

	private static void generateTmpAreaFullProductClickCountTable(SQLContext sqlContext) {
		String sql = "SELECT tapcc.area,tapcc.product_id,tapcc.click_count,tapcc.city_infos,pi.product_name,"
				+ "if(get_json_object(pi.extend_info,'product_status')='0','自营','第三方') product_status "
				+ "FROM tmp_area_product_click_count tapcc JOIN product_info pi ON tapcc.product_id=pi.product_id";
		DataFrame df = sqlContext.sql(sql);
		
		df.registerTempTable("tmp_area_fullprod_click_count");
	}
	
	/**
	 * 获取各区域top3热门商品
	 * @param sqlContext
	 * @return
	 */
	private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext){
		String sql = "SELECT area,CASE "
				+ "WHERE area='华北' OR area='华东' THEN 'A级' "
				+ "WHERE area='华南' OR area='华中' THEN 'B级' "
				+ "WHERE area='西北' OR area='西南' THEN 'C级' "
				+ "ELSE 'D级' END area_level "
				+ "product_id,click_count,city_infos,product_name,product_status "
				+ "FROM (SELECT area,product_id,click_count,city_infos,product_name,product_status,"
					+ "ROW_NUMBER() OVER(PARTITION BY area ORDER BY click_count DESC) rank "
				+ "FROM tmp_area_fullprod_click_count) t WHERE rank<=3";
		
		DataFrame df = sqlContext.sql(sql);
		return df.javaRDD();
	}
	
	private static void persistAreaTop3Product(long taskid,List<Row> rows){
		List<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();
		
		for(Row row : rows){
			AreaTop3Product areaTop3Product = new AreaTop3Product();
			areaTop3Product.setTaskid(taskid);
			areaTop3Product.setArea(row.getString(0));
			areaTop3Product.setAreaLevel(row.getString(1));
			areaTop3Product.setProductid(row.getLong(2));
			areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));
			areaTop3Product.setCityInfos(row.getString(4));
			areaTop3Product.setProductName(row.getString(5));
			areaTop3Product.setProductStatus(row.getString(6));
			areaTop3Products.add(areaTop3Product);
		}
		
		IAreaTop3ProductDAO areaTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
		areaTop3ProductDAO.insertBatch(areaTop3Products);
	}
}
