package com.lsy.sparkproject.spark.session;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.lsy.sparkproject.conf.ConfigurationManager;
import com.lsy.sparkproject.constant.Constants;
import com.lsy.sparkproject.dao.ISessionAggrStarDAO;
import com.lsy.sparkproject.dao.ISessionDetailDAO;
import com.lsy.sparkproject.dao.ISessionRandomExtractDAO;
import com.lsy.sparkproject.dao.ITaskDAO;
import com.lsy.sparkproject.dao.ITop10CategoryDAO;
import com.lsy.sparkproject.dao.ITop10SessionDAO;
import com.lsy.sparkproject.dao.factory.DAOFactory;
import com.lsy.sparkproject.domain.SessionAggrStat;
import com.lsy.sparkproject.domain.SessionDetail;
import com.lsy.sparkproject.domain.SessionRandomExtract;
import com.lsy.sparkproject.domain.Task;
import com.lsy.sparkproject.domain.Top10Category;
import com.lsy.sparkproject.domain.Top10Session;
import com.lsy.sparkproject.util.DateUtils;
import com.lsy.sparkproject.util.NumberUtils;
import com.lsy.sparkproject.util.ParamUtils;
import com.lsy.sparkproject.util.SparkUtils;
import com.lsy.sparkproject.util.StringUtils;
import com.lsy.sparkproject.util.ValidUtils;

/**
 * 用户访问session分析spark作业
 * 
 * 接受用户创建的分析任务，用户可能指定的条件： 1.时间范围 2.性别 3.年龄 4.职业 5.城市 6.搜索词 7.点击品类
 * 
 * 
 * @author work
 * 
 */
public class UserVisitSessionAnalyzeSpark {
	public static void main(String[] args) throws FileNotFoundException {

		// 构建Spark上下文
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
				.setMaster("local")
				.set("spark.serializer",
						"org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses(
						new Class[] { CategorySortKey.class, IntList.class });
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = getSQLContext(sc.sc());

		// 生成模拟测试数据
		SparkUtils.mackData(sc, sqlContext);

		// 创建需要使用的DAO组件
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();

		// 如果要进行session粒度的聚合，首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
		// 如果要根据用户在创建任务时指定的参数来进行数据过滤，就要首先查询出来指定的任务,并获取用户的查询参数
		long taskid = 2;
		Task task = taskDAO.findById(taskid);
		if (task == null) {
			System.out.println(new Date() + ": cannot find this task id ["
					+ taskid + "].");
			return;
		}
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

		/**
		 * actionRDD就是第一个公共RDD 第一：要用actionRDD获取到一个公共的sessionid为Key的pairRDD
		 * 第二：用在了sesison聚合环节
		 */
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext,
				taskParam);
		// System.out.println(actionRDD.take(10).toString());

		/*
		 * 获取数据到硬盘 BufferedOutputStream bufferedWriter = null; FileOutputStream
		 * fileOutputStream = null; try { fileOutputStream = new
		 * FileOutputStream("C:\\Users\\work\\Desktop\\date.txt"); } catch
		 * (FileNotFoundException e2) { e2.printStackTrace(); } bufferedWriter =
		 * new BufferedOutputStream(fileOutputStream);
		 * 
		 * List<Row> list = actionRDD.collect(); Iterator<Row> it =
		 * list.iterator(); while(it.hasNext()){ Row row = it.next(); String
		 * string = row.toString() + "\r\n"; byte[] buf = string.getBytes(); try
		 * { bufferedWriter.write(buf); bufferedWriter.flush(); } catch
		 * (IOException e) { e.printStackTrace(); } }
		 */

		JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);

		/**
		 * 持久化sessionid2actionRDD，就是调用persist（）方法，并传入持久化级别 1.MEMORY_ONLY
		 * 纯内存，无序列化 2.MEMORY_ONLY_SER 内存序列化 3.MEMORY_AND_DISK 纯内存和磁盘
		 * 4.MEMORY_AND_DISK_SER 内存和磁盘序列化 5.DISK_ONLY 纯磁盘
		 * 
		 * 如果内存充足，要使用双副本可靠机制 选择带后缀_2的策略，优先级同上
		 */
		sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel
				.MEMORY_ONLY());

		// 首先将行为数据按照session_id进行groupByKey分组
		// 此时数据粒度就是session粒度的了，可以将session粒度数据与用户信息数据进行join
		// 到这里为止，获取的数据是<sessionid,(sessionid,searchKeyWords,clickCategoryIds,age,professional,city,sex)>
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(
				sqlContext, sessionid2actionRDD);

		// System.out.println(sessionid2AggrInfoRDD.count());
		// for(Tuple2<String, String> tuple : sessionid2AggrInfoRDD.take(10)){
		// System.out.println(tuple._2);
		// }
		// 按照使用者指定的筛选参数进行数据过滤
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("",
				new SessionAggrStatAccumulator());
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
				sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);

		/**
		 * 持久化filteredSessionid2AggrInfoRDD
		 */
		filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD
				.persist(StorageLevel.MEMORY_ONLY());

		JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(
				filteredSessionid2AggrInfoRDD, sessionid2actionRDD);

		/**
		 * 持久化sessionid2detailRDD
		 */
		sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel
				.MEMORY_ONLY());

		randomExtractSession(sc, task.getTaskid(),
				filteredSessionid2AggrInfoRDD, sessionid2actionRDD);

		// 计算出各个范围的占比并写入mysql
		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),
				task.getTaskid());

		// System.out.println(filteredSessionid2AggrInfoRDD.count());
		// for(Tuple2<String, String> tuple :
		// filteredSessionid2AggrInfoRDD.take(10)){
		// System.out.println(tuple._2);
		// }

		// 获取top10热门品类
		List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(
				task.getTaskid(), sessionid2detailRDD);

		// 获取top10活跃session
		getTop10Session(sc, task.getTaskid(), top10CategoryList,
				sessionid2detailRDD);

		// 关闭spark上下文
		sc.close();
	}

	/**
	 * 获取SQLContext 如果实在本地测试运行，生成SQLContext对象 如果是在生产环境运行，那么生成HiveContext对象
	 * 
	 * @param SparkContext
	 * @return SQLContext
	 */
	private static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}

	/**
	 * 获取指定日期范围内的用户访问行为数据
	 * 
	 * @param sqlContext
	 * @param taskParam
	 * @return
	 */
	@SuppressWarnings("unused")
	private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext,
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

	/**
	 * 获取sessionid到访问行为数据的映射的RDD
	 * 
	 * @param actionRDD
	 * @return
	 */
	public static JavaPairRDD<String, Row> getSessionid2ActionRDD(
			JavaRDD<Row> actionRDD) {
		// return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Tuple2<String, Row> call(Row row) throws Exception {
		// return new Tuple2<String, Row>(row.getString(2), row);
		// }
		// });
		return actionRDD
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, Row>> call(Iterator<Row> t)
							throws Exception {
						List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
						while (t.hasNext()) {
							Row row = t.next();
							list.add(new Tuple2<String, Row>(row.getString(2),
									row));
						}
						return list;
					}
				});
	}

	/**
	 * 对行为数据按session粒度进行聚合
	 * 
	 * @param actionRDD
	 *            行为数据RDD
	 * @return session粒度聚合数据
	 */
	private static JavaPairRDD<String, String> aggregateBySession(
			SQLContext sqlContext, JavaPairRDD<String, Row> sessionid2actionRDD) {

		// 对行为数据按session粒度进行分组
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2actionRDD
				.groupByKey();

		// 对每一个session分组进行聚合
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD
				.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<String, Iterable<Row>> tuple)
							throws Exception {
						String sessionid = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();

						StringBuffer searchKeywordsBuffer = new StringBuffer("");
						StringBuffer clickCategoryIdsBuffer = new StringBuffer(
								"");

						Long userid = null;

						// session的起始和结束时间
						Date startTime = null;
						Date endTime = null;
						// session的访问步长
						int stepLength = 0;

						// 遍历session所有的访问行为
						while (iterator.hasNext()) {
							// 提取每个访问行为的搜索词地段和点击品类字段
							Row row = iterator.next();
							if (userid == null) {
								userid = row.getLong(1);
							}
							String searchKeyWord = row.getString(5);
							Long clickCategoryId = row.getLong(6);

							if (StringUtils.isNotEmpty(searchKeyWord)) {
								if (!searchKeywordsBuffer.toString().contains(
										searchKeyWord)) {
									searchKeywordsBuffer.append(searchKeyWord
											+ ",");
								}
							}
							if (clickCategoryId != null) {
								if (!clickCategoryIdsBuffer
										.toString()
										.contains(
												String.valueOf(clickCategoryId))) {
									clickCategoryIdsBuffer
											.append(clickCategoryId + ",");
								}
							}

							// 计算session开始和结束时间
							Date actionTime = DateUtils.parseTime(row
									.getString(4));
							if (startTime == null) {
								startTime = actionTime;
							}
							if (endTime == null) {
								endTime = actionTime;
							}
							if (actionTime.before(startTime)) {
								startTime = actionTime;
							}
							if (actionTime.after(endTime)) {
								endTime = actionTime;
							}

							// 计算session访问步长
							stepLength++;
						}

						String searchKeyWords = StringUtils
								.trimComma(searchKeywordsBuffer.toString());
						String clickCategoryIds = StringUtils
								.trimComma(clickCategoryIdsBuffer.toString());

						Long visitLength = (endTime.getTime() - startTime
								.getTime()) / 1000;
						// 拼接
						String partAggrInfo = Constants.FIELD_SESSION_ID + "="
								+ sessionid + "|"
								+ Constants.FIELD_SEARCH_KEYWORDS + "="
								+ searchKeyWords + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "="
								+ clickCategoryIds + "|"
								+ Constants.FIELD_VISIT_LENGTH + "="
								+ visitLength + "|"
								+ Constants.FIELD_STEP_LENGTH + "="
								+ stepLength + "|" + Constants.FIELD_START_TIME
								+ "=" + DateUtils.formatTime(startTime);
						return new Tuple2<Long, String>(userid, partAggrInfo);
					}
				});
		// 查询所有用户数据
		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD
				.mapToPair(new PairFunction<Row, Long, Row>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Row> call(Row row) throws Exception {

						return new Tuple2<Long, Row>(row.getLong(0), row);
					}
				});
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD
				.join(userid2InfoRDD);
		// 对join的数据进行拼接，并返回<sessionid,fullaggrInfo>格式数据
		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(
							Tuple2<Long, Tuple2<String, Row>> tuple)
							throws Exception {
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;

						String sessionid = StringUtils
								.getFieldFromConcatString(partAggrInfo, "\\|",
										Constants.FIELD_SESSION_ID);

						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);
						String fullAggrInfo = partAggrInfo + "|"
								+ Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "="
								+ professional + "|" + Constants.FIELD_CITY
								+ "=" + city + "|" + Constants.FIELD_SEX + "="
								+ sex;

						return new Tuple2<String, String>(sessionid,
								fullAggrInfo);
					}
				});

		// Sample采样倾斜key单独进行join
		// JavaPairRDD<Long, String> sampledRDD =
		// userid2PartAggrInfoRDD.sample(false, 0.1, 9);
		// JavaPairRDD<Long, Long> mappedSampledRDD = sampledRDD.mapToPair(new
		// PairFunction<Tuple2<Long,String>, Long, Long>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Tuple2<Long, Long> call(Tuple2<Long, String> t)
		// throws Exception {
		// return new Tuple2<Long, Long>(t._1, 1L);
		// }
		// });
		// JavaPairRDD<Long, Long> computedSampledRDD =
		// mappedSampledRDD.reduceByKey(new Function2<Long, Long, Long>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Long call(Long v1, Long v2) throws Exception {
		// return v1 + v2;
		// }
		// });
		// JavaPairRDD<Long, Long> reversedSampledRDD =
		// computedSampledRDD.mapToPair(new PairFunction<Tuple2<Long,Long>,
		// Long, Long>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Tuple2<Long, Long> call(Tuple2<Long, Long> t)
		// throws Exception {
		// return new Tuple2<Long, Long>(t._2, t._1);
		// }
		// });
		// final Long skewedUserid =
		// reversedSampledRDD.sortByKey(false).take(1).get(0)._2;
		// JavaPairRDD<Long, String> skewedRDD =
		// userid2PartAggrInfoRDD.filter(new Function<Tuple2<Long,String>,
		// Boolean>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Boolean call(Tuple2<Long, String> v1) throws Exception {
		// return v1._1.equals(skewedUserid);
		// }
		// });
		// JavaPairRDD<Long, String> commmonRDD =
		// userid2PartAggrInfoRDD.filter(new Function<Tuple2<Long,String>,
		// Boolean>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Boolean call(Tuple2<Long, String> v1) throws Exception {
		// return !v1._1.equals(skewedUserid);
		// }
		// });
		// JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD1 =
		// skewedRDD.join(userid2InfoRDD);
		// JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD2 =
		// commmonRDD.join(userid2InfoRDD);
		// JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD =
		// joinedRDD1.union(joinedRDD2);
		// JavaPairRDD<String, String> finalRDD = joinedRDD
		// .mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>,
		// String, String>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Tuple2<String, String> call(
		// Tuple2<Long, Tuple2<String, Row>> tuple)
		// throws Exception {
		// String partAggrInfo = tuple._2._1;
		// Row userInfoRow = tuple._2._2;
		//
		// String sessionid = StringUtils
		// .getFieldFromConcatString(partAggrInfo, "\\|",
		// Constants.FIELD_SESSION_ID);
		//
		// int age = userInfoRow.getInt(3);
		// String professional = userInfoRow.getString(4);
		// String city = userInfoRow.getString(5);
		// String sex = userInfoRow.getString(6);
		// String fullAggrInfo = partAggrInfo + "|"
		// + Constants.FIELD_AGE + "=" + age + "|"
		// + Constants.FIELD_PROFESSIONAL + "="
		// + professional + "|" + Constants.FIELD_CITY
		// + "=" + city + "|" + Constants.FIELD_SEX + "="
		// + sex;
		//
		// return new Tuple2<String, String>(sessionid,
		// fullAggrInfo);
		// }
		// });

		return sessionid2FullAggrInfoRDD;

	}

	/**
	 * 过滤session数据
	 * 
	 * @param sessionid2AggrInfoRDD
	 * @return
	 */
	private static JavaPairRDD<String, String> filterSessionAndAggrStat(
			JavaPairRDD<String, String> sessionid2AggrInfoRDD,
			final JSONObject taskParam,
			final Accumulator<String> sessionAggrStatAccumulator) {

		String startAge = ParamUtils.getParam(taskParam,
				Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam,
				Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keyWords = ParamUtils.getParam(taskParam,
				Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam,
				Constants.PARAM_CATEGORY_IDS);
		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "="
				+ startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge
						+ "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "="
						+ professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|"
						: "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keyWords != null ? Constants.PARAM_KEYWORDS + "=" + keyWords
						+ "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "="
						+ categoryIds : "");
		if (_parameter.endsWith("\\|")) {
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}

		final String parameter = _parameter;

		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD
				.filter(new Function<Tuple2<String, String>, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, String> tuple)
							throws Exception {
						// 首先从tuple中获取聚合数据
						String aggrInfo = tuple._2;

						// 按照年龄范围进行过滤
						if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
								parameter, Constants.PARAM_START_AGE,
								Constants.PARAM_END_AGE)) {
							return false;
						}

						// 按照职业范围进行过滤
						if (!ValidUtils.in(aggrInfo,
								Constants.FIELD_PROFESSIONAL, parameter,
								Constants.PARAM_PROFESSIONALS)) {
							return false;
						}

						// 按照城市范围进行过滤
						if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
								parameter, Constants.PARAM_CITIES)) {
							return false;
						}

						// 按照性别进行过滤
						if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
								parameter, Constants.PARAM_SEX)) {
							return false;
						}

						// 按照搜索词进行过滤
						if (!ValidUtils.in(aggrInfo,
								Constants.FIELD_SEARCH_KEYWORDS, parameter,
								Constants.PARAM_KEYWORDS)) {
							return false;
						}

						// 按照点击品类id进行过滤
						if (!ValidUtils.in(aggrInfo,
								Constants.FIELD_CLICK_CATEGORY_IDS, parameter,
								Constants.PARAM_CATEGORY_IDS)) {
							return false;
						}

						// 进行累加计数
						sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

						// 计算出session访问时长和访问步长的范围，并进行相应累加
						long visitLength = Long.valueOf(StringUtils
								.getFieldFromConcatString(aggrInfo, "\\|",
										Constants.FIELD_VISIT_LENGTH));
						long stepLength = Long.valueOf(StringUtils
								.getFieldFromConcatString(aggrInfo, "\\|",
										Constants.FIELD_STEP_LENGTH));
						calculateVisitLength(visitLength);
						calulateStepLength(stepLength);
						return true;
					}

					/**
					 * 计算访问时长范围
					 * 
					 * @param visitLength
					 */
					private void calculateVisitLength(long visitLength) {
						if (visitLength >= 1 && visitLength <= 3) {
							sessionAggrStatAccumulator
									.add(Constants.TIME_PERIOD_1s_3s);
						} else if (visitLength >= 4 && visitLength <= 6) {
							sessionAggrStatAccumulator
									.add(Constants.TIME_PERIOD_4s_6s);
						} else if (visitLength >= 7 && visitLength <= 9) {
							sessionAggrStatAccumulator
									.add(Constants.TIME_PERIOD_7s_9s);
						} else if (visitLength >= 10 && visitLength <= 30) {
							sessionAggrStatAccumulator
									.add(Constants.TIME_PERIOD_10s_30s);
						} else if (visitLength > 30 && visitLength <= 60) {
							sessionAggrStatAccumulator
									.add(Constants.TIME_PERIOD_30s_60s);
						} else if (visitLength > 60 && visitLength <= 180) {
							sessionAggrStatAccumulator
									.add(Constants.TIME_PERIOD_1m_3m);
						} else if (visitLength > 180 && visitLength <= 600) {
							sessionAggrStatAccumulator
									.add(Constants.TIME_PERIOD_3m_10m);
						} else if (visitLength > 600 && visitLength <= 1800) {
							sessionAggrStatAccumulator
									.add(Constants.TIME_PERIOD_10m_30m);
						} else if (visitLength > 1800) {
							sessionAggrStatAccumulator
									.add(Constants.TIME_PERIOD_30m);
						}
					}

					private void calulateStepLength(long stepLength) {
						if (stepLength >= 1 && stepLength <= 3) {
							sessionAggrStatAccumulator
									.add(Constants.STEP_PERIOD_1_3);
						} else if (stepLength >= 4 && stepLength <= 6) {
							sessionAggrStatAccumulator
									.add(Constants.STEP_PERIOD_4_6);
						} else if (stepLength >= 7 && stepLength <= 9) {
							sessionAggrStatAccumulator
									.add(Constants.STEP_PERIOD_7_9);
						} else if (stepLength >= 10 && stepLength <= 30) {
							sessionAggrStatAccumulator
									.add(Constants.STEP_PERIOD_10_30);
						} else if (stepLength > 30 && stepLength <= 60) {
							sessionAggrStatAccumulator
									.add(Constants.STEP_PERIOD_30_60);
						} else if (stepLength > 60) {
							sessionAggrStatAccumulator
									.add(Constants.STEP_PERIOD_60);
						}
					}
				});
		return filteredSessionid2AggrInfoRDD;

	}

	/**
	 * 获取通过条件的session访问明细数据RDD
	 * 
	 * @param sessionid2aggrInfoRDD
	 * @param sessionid2actionRDD
	 * @return
	 */
	private static JavaPairRDD<String, Row> getSessionid2detailRDD(
			JavaPairRDD<String, String> sessionid2aggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) {
		JavaPairRDD<String, Row> sessionid2detailRDD = sessionid2aggrInfoRDD
				.join(sessionid2actionRDD)
				.mapToPair(
						new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {

							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<String, Row> call(
									Tuple2<String, Tuple2<String, Row>> t)
									throws Exception {
								return new Tuple2<String, Row>(t._1, t._2._2);
							}
						});
		return sessionid2detailRDD;
	}

	/**
	 * 随机抽取session
	 * 
	 * @param sessionid2AggrInfoRDD
	 * @param sessionid2actionRDD
	 */
	private static void randomExtractSession(JavaSparkContext sc,
			final long taskid,
			JavaPairRDD<String, String> sessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) {
		// 第一步：计算出每天每小时的session数量
		JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(
							Tuple2<String, String> tuple) throws Exception {
						String agggrInfo = tuple._2;
						String startTime = StringUtils
								.getFieldFromConcatString(agggrInfo, "\\|",
										Constants.FIELD_START_TIME);
						String dateHour = DateUtils.getDateHour(startTime);
						return new Tuple2<String, String>(dateHour, agggrInfo);
					}
				});
		Map<String, Object> countMap = time2sessionidRDD.countByKey();

		// 第二步：使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
		// 将<yyyy-mm-dd_hh,count>数据变成<yyyy-mm-dd,<hh,count>>
		Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();
		for (Map.Entry<String, Object> countEntry : countMap.entrySet()) {
			String dateHour = countEntry.getKey();
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];
			long count = Long.valueOf(String.valueOf(countEntry.getValue()));
			Map<String, Long> hourCountMap = dateHourCountMap.get(date);
			if (hourCountMap == null) {
				hourCountMap = new HashMap<String, Long>();
				dateHourCountMap.put(date, hourCountMap);
			}
			hourCountMap.put(hour, count);
		}

		int extractNumberPerDay = 100 / dateHourCountMap.size();
		// <date,<hour,(3,5,20,103)>>
		Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();
		for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap
				.entrySet()) {
			String date = dateHourCountEntry.getKey();
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

			// 一天的session总数
			long sessionCount = 0L;
			for (long hourCount : hourCountMap.values()) {
				sessionCount += hourCount;
			}

			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap
					.get(date);
			if (hourExtractMap == null) {
				hourExtractMap = new HashMap<String, List<Integer>>();
				dateHourExtractMap.put(date, hourExtractMap);
			}

			// 遍历每个小时
			for (Map.Entry<String, Long> hourCountEntry : hourCountMap
					.entrySet()) {
				String hour = hourCountEntry.getKey();
				int count = Integer.valueOf(String.valueOf(hourCountEntry
						.getValue()));
				int hourExtractNumber = (int) (((double) count / (double) sessionCount) * extractNumberPerDay);
				if (hourExtractNumber > count) {
					hourExtractNumber = count;
				}
				List<Integer> extractIndexList = hourExtractMap.get(hour);
				if (extractIndexList == null) {
					extractIndexList = new ArrayList<Integer>();
					hourExtractMap.put(hour, extractIndexList);
				}
				for (int i = 0; i < hourExtractNumber; i++) {
					int extractIndex = new Random().nextInt(count);
					while (extractIndexList.contains(extractIndex)) {
						extractIndex = new Random().nextInt(count);
					}
					extractIndexList.add(extractIndex);
				}
			}
		}

		/**
		 * 广播变量dateHourExtractMap 使用广播变量需要掉用value（）方法
		 * fastutil，比如List<Integer>的list对应fastutil的IntList
		 */
		Map<String, Map<String, IntList>> fastutilDateHourExtractMap = new HashMap<String, Map<String, IntList>>();
		for (Map.Entry<String, Map<String, List<Integer>>> dateHourExtractentry : dateHourExtractMap
				.entrySet()) {
			String date = dateHourExtractentry.getKey();
			Map<String, List<Integer>> hourExtractMap = dateHourExtractentry
					.getValue();
			Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();
			for (Map.Entry<String, List<Integer>> hourExtractentry : hourExtractMap
					.entrySet()) {
				String hour = hourExtractentry.getKey();
				List<Integer> extractList = hourExtractentry.getValue();
				IntList fastutilExtractList = new IntArrayList();
				for (int i = 0; i < extractList.size(); i++) {
					fastutilExtractList.add(extractList.get(i));
				}

				fastutilHourExtractMap.put(hour, fastutilExtractList);
			}
			fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
		}

		final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast = sc
				.broadcast(fastutilDateHourExtractMap);

		// 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
		// <dateHour,(session,aggrinfo)>
		JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD
				.groupByKey();
		JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, String>> call(
							Tuple2<String, Iterable<String>> t)
							throws Exception {
						List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();

						String dateHour = t._1;
						String date = dateHour.split("_")[0];
						String hour = dateHour.split("_")[1];
						Iterator<String> iterator = t._2.iterator();

						Map<String, Map<String, IntList>> dateHourExtractMap = dateHourExtractMapBroadcast
								.value();

						List<Integer> extractIndexList = dateHourExtractMap
								.get(date).get(hour);
						ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory
								.getSessionRandomExtractDAO();
						int index = 0;
						while (iterator.hasNext()) {
							String sessionAggrInfo = iterator.next();
							String sessionid = StringUtils
									.getFieldFromConcatString(sessionAggrInfo,
											"\\|", Constants.FIELD_SESSION_ID);
							if (extractIndexList.contains(index)) {
								SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
								sessionRandomExtract.setTaskid(taskid);
								sessionRandomExtract.setSessionid(sessionid);
								sessionRandomExtract.setStartTime(StringUtils
										.getFieldFromConcatString(
												sessionAggrInfo, "\\|",
												Constants.FIELD_START_TIME));
								sessionRandomExtract.setSearchKeywords(StringUtils
										.getFieldFromConcatString(
												sessionAggrInfo, "\\|",
												Constants.FIELD_SEARCH_KEYWORDS));
								sessionRandomExtract.setClickCategoryIds(StringUtils
										.getFieldFromConcatString(
												sessionAggrInfo,
												"\\|",
												Constants.FIELD_CLICK_CATEGORY_IDS));
								sessionRandomExtractDAO
										.insert(sessionRandomExtract);

								extractSessionids
										.add(new Tuple2<String, String>(
												sessionid, sessionid));
							}
							index++;
						}
						return extractSessionids;
					}
				});

		JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionidsRDD
				.join(sessionid2actionRDD);
		// extractSessionDetailRDD
		// .foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public void call(Tuple2<String, Tuple2<String, Row>> t)
		// throws Exception {
		// Row row = t._2._2;
		//
		// SessionDetail sessionDetail = new SessionDetail();
		// sessionDetail.setTaskid(taskid);
		// sessionDetail.setUserid(row.getLong(1));
		// sessionDetail.setSessionid(row.getString(2));
		// sessionDetail.setPageid(row.getLong(3));
		// sessionDetail.setActionTime(row.getString(4));
		// sessionDetail.setSearchKeyword(row.getString(5));
		// sessionDetail.setClickCategoryId(row.getLong(6));
		// sessionDetail.setClickProductId(row.getLong(7));
		// sessionDetail.setOrderCategoryIds(row.getString(8));
		// sessionDetail.setOrderProductIds(row.getString(9));
		// sessionDetail.setPayCategoryIds(row.getString(10));
		// sessionDetail.setPayProductIds(row.getString(11));
		//
		// ISessionDetailDAO sessionDetailDAO = DAOFactory
		// .getSessionDetailDAO();
		// sessionDetailDAO.insert(sessionDetail);
		// }
		// });

		extractSessionDetailRDD
				.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(
							Iterator<Tuple2<String, Tuple2<String, Row>>> t)
							throws Exception {
						List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();
						while (t.hasNext()) {
							Tuple2<String, Tuple2<String, Row>> tuple2 = t
									.next();
							Row row = tuple2._2._2;

							SessionDetail sessionDetail = new SessionDetail();
							sessionDetail.setTaskid(taskid);
							sessionDetail.setUserid(row.getLong(1));
							sessionDetail.setSessionid(row.getString(2));
							sessionDetail.setPageid(row.getLong(3));
							sessionDetail.setActionTime(row.getString(4));
							sessionDetail.setSearchKeyword(row.getString(5));
							sessionDetail.setClickCategoryId(row.getLong(6));
							sessionDetail.setClickProductId(row.getLong(7));
							sessionDetail.setOrderCategoryIds(row.getString(8));
							sessionDetail.setOrderProductIds(row.getString(9));
							sessionDetail.setPayCategoryIds(row.getString(10));
							sessionDetail.setPayProductIds(row.getString(11));

							sessionDetails.add(sessionDetail);
						}

						ISessionDetailDAO sessionDetailDAO = DAOFactory
								.getSessionDetailDAO();
						sessionDetailDAO.insertBatch(sessionDetails);

					}
				});

	}

	/**
	 * 计算session范围占比，并写入mysql
	 * 
	 * @param value
	 */
	private static void calculateAndPersistAggrStat(String value, long taskid) {
		long session_Count = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.SESSION_COUNT));
		long visit_length_1s_3s = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.TIME_PERIOD_1s_3s));
		long visit_length_4s_6s = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.TIME_PERIOD_30m));

		long step_length_1_3 = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long.valueOf(StringUtils
				.getFieldFromConcatString(value, "\\|",
						Constants.STEP_PERIOD_60));

		double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
				(double) visit_length_1s_3s / (double) session_Count, 2);
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
				(double) visit_length_4s_6s / (double) session_Count, 2);
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
				(double) visit_length_7s_9s / (double) session_Count, 2);
		double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
				(double) visit_length_10s_30s / (double) session_Count, 2);
		double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
				(double) visit_length_30s_60s / (double) session_Count, 2);
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
				(double) visit_length_1m_3m / (double) session_Count, 2);
		double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
				(double) visit_length_3m_10m / (double) session_Count, 2);
		double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
				(double) visit_length_10m_30m / (double) session_Count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble(
				(double) visit_length_30m / (double) session_Count, 2);

		double step_length_1_3_ratio = NumberUtils.formatDouble(
				(double) step_length_1_3 / (double) session_Count, 2);
		double step_length_4_6_ratio = NumberUtils.formatDouble(
				(double) step_length_4_6 / (double) session_Count, 2);
		double step_length_7_9_ratio = NumberUtils.formatDouble(
				(double) step_length_7_9 / (double) session_Count, 2);
		double step_length_10_30_ratio = NumberUtils.formatDouble(
				(double) step_length_10_30 / (double) session_Count, 2);
		double step_length_30_60_ratio = NumberUtils.formatDouble(
				(double) step_length_30_60 / (double) session_Count, 2);
		double step_length_60_ratio = NumberUtils.formatDouble(
				(double) step_length_60 / (double) session_Count, 2);

		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(session_Count);
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
		sessionAggrStat
				.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
		sessionAggrStat
				.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
		sessionAggrStat
				.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

		ISessionAggrStarDAO sessionAggrStarDAO = DAOFactory
				.getSessionAggrStatDAO();
		sessionAggrStarDAO.insert(sessionAggrStat);
	}

	/**
	 * 获取top10热门品类
	 * 
	 * @param filteredSessionid2AggrInfoRDD
	 * @param sessionid2actionRDD
	 */
	private static List<Tuple2<CategorySortKey, String>> getTop10Category(
			long taskid, JavaPairRDD<String, Row> sessionid2detailRDD) {
		/**
		 * 第一步：获取符合条件的session访问过的所有品类
		 */
		// 获取session访问过的所有品类id
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<String, Row> t) throws Exception {
						Row row = t._2;
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						Long clickCategoryId = row.getLong(6);
						if (clickCategoryId != null) {
							list.add(new Tuple2<Long, Long>(clickCategoryId,
									clickCategoryId));
						}
						String orderCategoryIds = row.getString(8);
						if (orderCategoryIds != null) {
							String[] orderCategoryIdsSplited = orderCategoryIds
									.split(",");
							for (String orderCategoryId : orderCategoryIdsSplited) {
								list.add(new Tuple2<Long, Long>(Long
										.valueOf(orderCategoryId), Long
										.valueOf(orderCategoryId)));
							}
						}
						String payCategoryIds = row.getString(10);
						if (payCategoryIds != null) {
							String[] payCategoryIdsSplited = payCategoryIds
									.split(",");
							for (String payCategoryId : payCategoryIdsSplited) {
								list.add(new Tuple2<Long, Long>(Long
										.valueOf(payCategoryId), Long
										.valueOf(payCategoryId)));
							}
						}
						return list;
					}
				});

		/**
		 * 必须进行去重，否则会出现重复的categoryid，排序会对重复的categoryid以及caountinfo进行排序
		 */
		categoryidRDD = categoryidRDD.distinct();

		/**
		 * 第二步：计算各品类的点击、下单和支付次数
		 */
		// 计算各个品类的点击次数
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2detailRDD);

		// 计算各个下单的次数
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2detailRDD);

		// 计算各个支付的次数
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD);

		/**
		 * 第三步：join各品类与他的点击、下单和支付次数
		 */
		JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndDate(
				categoryidRDD, clickCategoryId2CountRDD,
				orderCategoryId2CountRDD, payCategoryId2CountRDD);

		/**
		 * 第四步：自定义二次排序key
		 */

		/**
		 * 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序
		 */
		JavaPairRDD<CategorySortKey, String> sortKey2CountRDD = categoryid2countRDD
				.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<CategorySortKey, String> call(
							Tuple2<Long, String> t) throws Exception {
						String countInfo = t._2;
						long clickCount = Long.valueOf(StringUtils
								.getFieldFromConcatString(countInfo, "\\|",
										Constants.FIELD_CLICK_COUNT));
						long orderCount = Long.valueOf(StringUtils
								.getFieldFromConcatString(countInfo, "\\|",
										Constants.FIELD_ORDER_COUNT));
						long payCount = Long.valueOf(StringUtils
								.getFieldFromConcatString(countInfo, "\\|",
										Constants.FIELD_PAY_COUNT));
						CategorySortKey sortKey = new CategorySortKey(
								clickCount, orderCount, payCount);
						return new Tuple2<CategorySortKey, String>(sortKey,
								countInfo);
					}
				});

		JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2CountRDD
				.sortByKey(true);

		/**
		 * 第六步：用take（10）取出top10热门商品，并写入mysql
		 */
		List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD
				.take(10);
		ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
		for (Tuple2<CategorySortKey, String> t : top10CategoryList) {
			String countInfo = t._2;
			long categoryid = Long.valueOf(StringUtils
					.getFieldFromConcatString(countInfo, "\\|",
							Constants.FIELD_CATEGORY_ID));
			long clickCount = Long.valueOf(StringUtils
					.getFieldFromConcatString(countInfo, "\\|",
							Constants.FIELD_CLICK_COUNT));
			long orderCount = Long.valueOf(StringUtils
					.getFieldFromConcatString(countInfo, "\\|",
							Constants.FIELD_ORDER_COUNT));
			long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
					countInfo, "\\|", Constants.FIELD_PAY_COUNT));

			Top10Category category = new Top10Category();
			category.setTaskid(taskid);
			category.setCategoryid(categoryid);
			category.setClickcount(clickCount);
			category.setOrdercount(orderCount);
			category.setPaycount(payCount);

			top10CategoryDAO.insert(category);
		}
		return top10CategoryList;
	}

	/**
	 * 获取各品类点击次数RDD
	 * 
	 * @param sessionid2detailRDD
	 * @return clickCategoryId2CountRDD
	 */
	private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD
				.filter(new Function<Tuple2<String, Row>, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Row> v1)
							throws Exception {
						Row row = v1._2;
						return row.get(6) != null ? true : false;
					}
				});
		// .coalesce(100); local模式不用设置，本省自己优化了

		JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD
				.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Long> call(Tuple2<String, Row> t)
							throws Exception {
						long clickCategoryId = t._2.getLong(6);
						return new Tuple2<Long, Long>(clickCategoryId, 1L);
					}
				});

		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD
				.reduceByKey(new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
		// 使用随机key实现双重聚合
		// /**
		// * 第一步：给每个key打上一个随机数
		// */
		// JavaPairRDD<String, Long> mappedClickCategoryIdRDD =
		// clickCategoryId2CountRDD.mapToPair(new
		// PairFunction<Tuple2<Long,Long>, String, Long>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Tuple2<String, Long> call(Tuple2<Long, Long> t)
		// throws Exception {
		// Random random = new Random();
		// int prefix = random.nextInt(10);
		// return new Tuple2<String, Long>(prefix + "_" + t._1, t._2);
		// }
		// });
		//
		// /**
		// * 第二步：执行第一轮局部聚合
		// */
		// JavaPairRDD<String, Long> firstAggrRDD =
		// mappedClickCategoryIdRDD.reduceByKey(new Function2<Long, Long,
		// Long>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Long call(Long v1, Long v2) throws Exception {
		// return v1 + v2;
		// }
		// });
		//
		// /**
		// * 第三步：去掉每个key的前缀
		// */
		// JavaPairRDD<Long, Long> restoredRDD = firstAggrRDD.mapToPair(new
		// PairFunction<Tuple2<String,Long>, Long, Long>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Tuple2<Long, Long> call(Tuple2<String, Long> t)
		// throws Exception {
		// long categoryId = Long.valueOf(t._1.split("_")[1]);
		// return new Tuple2<Long, Long>(categoryId, t._2);
		// }
		// });
		//
		// /**
		// * 第四步：执行第二轮全部聚合
		// */
		// JavaPairRDD<Long, Long> globaAggrRDD = restoredRDD.reduceByKey(new
		// Function2<Long, Long, Long>() {
		//
		// private static final long serialVersionUID = 1L;
		//
		// @Override
		// public Long call(Long v1, Long v2) throws Exception {
		// return v1 + v2;
		// }
		// });

		return clickCategoryId2CountRDD;
	}

	/**
	 * 获取各品类下单次数RDD
	 * 
	 * @param sessionid2detailRDD
	 * @return orderCategoryId2CountRDD
	 */
	private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD
				.filter(new Function<Tuple2<String, Row>, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Row> v1)
							throws Exception {
						return v1._2.getString(8) != null ? true : false;
					}
				});

		JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<String, Row> t) throws Exception {
						Row row = t._2;
						String orderCategoryIds = row.getString(8);
						String[] orderCategoryIdsSplited = orderCategoryIds
								.split(",");
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						for (String orderCategoryId : orderCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long
									.valueOf(orderCategoryId), 1L));
						}
						return list;
					}
				});

		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD
				.reduceByKey(new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
		return orderCategoryId2CountRDD;
	}

	/**
	 * 获取各品类支付次数RDD
	 * 
	 * @param sessionid2detailRDD
	 * @return payCategoryId2CountRDD
	 */
	private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD
				.filter(new Function<Tuple2<String, Row>, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Row> v1)
							throws Exception {
						return v1._2.getString(10) != null ? true : false;
					}
				});

		JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<String, Row> t) throws Exception {
						Row row = t._2;
						String payCategoryIds = row.getString(10);
						String[] payCategoryIdsSplited = payCategoryIds
								.split(",");
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						for (String payCategoryId : payCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long
									.valueOf(payCategoryId), 1L));
						}
						return list;
					}
				});

		JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD
				.reduceByKey(new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});
		return payCategoryId2CountRDD;
	}

	/**
	 * 连接品类RDD与数据RDD
	 * 
	 * @param categoryidRDD
	 * @param clickCategoryId2CountRDD
	 * @param OrderCategoryId2CountRDD
	 * @param PayCategoryId2CountRDD
	 * @return tmpJoinRDD
	 */
	private static JavaPairRDD<Long, String> joinCategoryAndDate(
			JavaPairRDD<Long, Long> categoryidRDD,
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
			JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
			JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
		JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = categoryidRDD
				.leftOuterJoin(clickCategoryId2CountRDD);
		JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<Long, Optional<Long>>> t)
							throws Exception {
						long categoryid = t._1;
						Optional<Long> clickCountOptional = t._2._2;
						long clickCount = 0L;
						if (clickCountOptional.isPresent()) {
							clickCount = clickCountOptional.get();
						}
						String value = Constants.FIELD_CATEGORY_ID + "="
								+ categoryid + "|"
								+ Constants.FIELD_CLICK_COUNT + "="
								+ clickCount;
						return new Tuple2<Long, String>(categoryid, value);
					}
				});

		tmpMapRDD = tmpMapRDD
				.leftOuterJoin(orderCategoryId2CountRDD)
				.mapToPair(
						new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {

							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<Long, String> call(
									Tuple2<Long, Tuple2<String, Optional<Long>>> t)
									throws Exception {
								long categoryid = t._1;
								String value = t._2._1;
								Optional<Long> orderCountOptional = t._2._2;
								long orderCount = 0L;
								if (orderCountOptional != null) {
									orderCount = orderCountOptional.get();
								}
								value = value + "|"
										+ Constants.FIELD_ORDER_COUNT + "="
										+ orderCount;
								return new Tuple2<Long, String>(categoryid,
										value);
							}
						});

		tmpMapRDD = tmpMapRDD
				.leftOuterJoin(payCategoryId2CountRDD)
				.mapToPair(
						new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {

							private static final long serialVersionUID = 1L;

							@Override
							public Tuple2<Long, String> call(
									Tuple2<Long, Tuple2<String, Optional<Long>>> t)
									throws Exception {
								long categoryid = t._1;
								Optional<Long> payCountOptional = t._2._2;
								String value = t._2._1;
								long payCount = 0L;
								if (payCountOptional != null) {
									payCount = payCountOptional.get();
								}
								value = value + "|" + Constants.FIELD_PAY_COUNT
										+ "=" + payCount;
								return new Tuple2<Long, String>(categoryid,
										value);
							}
						});
		return tmpMapRDD;

	}

	/**
	 * 
	 * @param sc
	 * @param taskid
	 * @param top10CategoryList
	 * @param sessionid2detailRDD
	 */
	private static void getTop10Session(JavaSparkContext sc, final long taskid,
			List<Tuple2<CategorySortKey, String>> top10CategoryList,
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		/**
		 * 第一步：将top10热门品类id生成一份RDD
		 */
		List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<Tuple2<Long, Long>>();
		for (Tuple2<CategorySortKey, String> category : top10CategoryList) {
			long categoryid = Long.valueOf(StringUtils
					.getFieldFromConcatString(category._2, "\\|",
							Constants.FIELD_CATEGORY_ID));
			top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid,
					categoryid));
		}
		JavaPairRDD<Long, Long> top10CategoryIdRDD = sc
				.parallelizePairs(top10CategoryIdList);

		/**
		 * 第二步：计算top10品类被各session点击的次数
		 */

		JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD = sessionid2detailRDD
				.groupByKey();

		JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, String>> call(
							Tuple2<String, Iterable<Row>> t) throws Exception {
						String sessionid = t._1;
						Iterator<Row> iterator = t._2.iterator();

						Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
						// 计算出该session对每个品类的点击次数
						while (iterator.hasNext()) {
							Row row = iterator.next();
							if (row.get(6) != null) {
								long categoryid = row.getLong(6);
								Long count = categoryCountMap.get(categoryid);
								if (count == null) {
									count = 0L;
								}

								count++;
								categoryCountMap.put(categoryid, count);
							}
						}

						// 返回格式<categoryid，sessionid：count>
						List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
						for (Entry<Long, Long> categoryCountEntry : categoryCountMap
								.entrySet()) {
							long categoryid = categoryCountEntry.getKey();
							long count = categoryCountEntry.getValue();
							String value = sessionid + "," + count;
							list.add(new Tuple2<Long, String>(categoryid, value));
						}
						return list;
					}
				});

		// top10热门品类被各个session点击次数
		JavaPairRDD<Long, Tuple2<Long, String>> top10CategorySessionCountRDDtmp = top10CategoryIdRDD
				.join(categoryid2sessionCountRDD);

		JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategorySessionCountRDDtmp
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<Long, String>> t)
							throws Exception {
						return new Tuple2<Long, String>(t._1, t._2._2);
					}
				});

		/**
		 * 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
		 */
		JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = top10CategorySessionCountRDD
				.groupByKey();

		JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, String>> call(
							Tuple2<Long, Iterable<String>> t) throws Exception {
						long categoryid = t._1;
						Iterator<String> iterator = t._2.iterator();
						String[] top10Sessions = new String[10];
						while (iterator.hasNext()) {
							String sessionCount = iterator.next();
							long count = Long.valueOf(sessionCount.split(",")[1]);

							for (int i = 0; i < top10Sessions.length; i++) {
								if (top10Sessions[i] == null) {
									top10Sessions[i] = sessionCount;
									break;
								} else {
									long _count = Long.valueOf(top10Sessions[i]
											.split(",")[1]);

									if (count > _count) {
										for (int j = 9; j > i; j--) {
											top10Sessions[j] = top10Sessions[j - 1];
										}
										top10Sessions[i] = sessionCount;
										break;
									}
								}
							}
						}

						// 将数据写入mysql表
						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

						for (String sessionCount : top10Sessions) {
							if (sessionCount != null) {
								String sessionid = sessionCount.split(",")[0];
								long count = Long.valueOf(sessionCount
										.split(",")[1]);
								Top10Session top10Session = new Top10Session();
								top10Session.setTaskid(taskid);
								top10Session.setCategoryid(categoryid);
								top10Session.setSessionid(sessionid);
								top10Session.setClickCount(count);

								ITop10SessionDAO top10SessionDAO = DAOFactory
										.getTop10SessionDAO();
								top10SessionDAO.insert(top10Session);
								list.add(new Tuple2<String, String>(sessionid,
										sessionid));

							}
						}
						return list;
					}
				});

		/**
		 * 第四步：获取top10活跃session的明细数据，并写入mysql
		 */
		JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD = top10SessionRDD
				.join(sessionid2detailRDD);
		sessionDetailRDD
				.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, Tuple2<String, Row>> t)
							throws Exception {
						Row row = t._2._2;

						SessionDetail sessionDetail = new SessionDetail();
						sessionDetail.setTaskid(taskid);
						sessionDetail.setUserid(row.getLong(1));
						sessionDetail.setSessionid(row.getString(2));
						sessionDetail.setPageid(row.getLong(3));
						sessionDetail.setActionTime(row.getString(4));
						sessionDetail.setSearchKeyword(row.getString(5));
						sessionDetail.setClickCategoryId(row.getLong(6));
						sessionDetail.setClickProductId(row.getLong(7));
						sessionDetail.setOrderCategoryIds(row.getString(8));
						sessionDetail.setOrderProductIds(row.getString(9));
						sessionDetail.setPayCategoryIds(row.getString(10));
						sessionDetail.setPayProductIds(row.getString(11));

						ISessionDetailDAO sessionDetailDAO = DAOFactory
								.getSessionDetailDAO();
						sessionDetailDAO.insert(sessionDetail);
					}
				});
	}
}
