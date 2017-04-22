//归一化
val result1 = h.sql("select max(consume_level) from model_input_losswarning_t ")//最大消费等级
val max_consume_level = result1.collect()(0).get(0).asInstanceOf[Integer].toDouble
val result2 = h.sql("select min(consume_level) from model_input_losswarning_t ") //最小消费等级
val min_consume_level = result2.collect()(0).get(0).asInstanceOf[Integer].toDouble
val region_consume_level = if((max_consume_level - min_consume_level ) == 0) 1 else (max_consume_level - min_consume_level )

val result3 = h.sql("select max(n_complain) from model_input_losswarning_t ")//最大投诉次数
val max_n_complain = result3.collect()(0).get(0).asInstanceOf[Integer].toDouble
val result4 = h.sql("select min(n_complain) from model_input_losswarning_t ") //最小投诉次数
val min_n_complain = result4.collect()(0).get(0).asInstanceOf[Integer].toDouble
val region_n_complain = if((max_n_complain - min_n_complain ) == 0) 1 else (max_n_complain - min_n_complain )

val result5 = h.sql("select max(n_refund ) from model_input_losswarning_t ")//最大退货次数
val max_n_refund  = result5.collect()(0).get(0).asInstanceOf[Integer].toDouble
val result6 = h.sql("select min(n_refund) from model_input_losswarning_t ") //最小退货次数
val min_n_refund  = result6.collect()(0).get(0).asInstanceOf[Integer].toDouble
val region_n_refund= if((max_n_refund - min_n_refund ) == 0) 1 else (max_n_refund - min_n_refund )

val result7 = h.sql("select max(total_days) from model_input_losswarning_t ")//最大累计访问天数
val max_total_days  = result7.collect()(0).get(0).asInstanceOf[Integer].toDouble
val result8 = h.sql("select min(total_days) from model_input_losswarning_t ") //最小累计访问天数
val min_total_days   = result8.collect()(0).get(0).asInstanceOf[Integer].toDouble
val region_total_days = if((max_total_days  - min_total_days   ) == 0) 1 else (max_total_days  - min_total_days   )

val result9 = h.sql("select max(total_time) from model_input_losswarning_t ")//最大累计访问时长
val max_total_time  = result9.collect()(0).get(0).asInstanceOf[Float].toDouble
val result10 = h.sql("select min(total_time) from model_input_losswarning_t ") //最小累计访问时长
val min_total_time   = result10.collect()(0).get(0).asInstanceOf[Float].toDouble
val region_total_time = if((max_total_time  - min_total_time   ) == 0) 1 else (max_total_time  - min_total_time   )

val result11 = h.sql("select max(n_phurchase) from model_input_losswarning_t ")//最大购买次数
val max_n_phurchase  = result11.collect()(0).get(0).asInstanceOf[Integer].toDouble
val result12 = h.sql("select min(n_phurchase) from model_input_losswarning_t ") //最小购买次数
val min_n_phurchase   = result12.collect()(0).get(0).asInstanceOf[Integer].toDouble
val region_n_phurchase = if((max_n_phurchase  - min_n_phurchase   ) == 0) 1 else (max_n_phurchase  - min_n_phurchase   )

val result13 = h.sql("select max(amount ) from model_input_losswarning_t ")//最大购买金额
val max_amount   = result13.collect()(0).get(0).asInstanceOf[Integer].toDouble
val result14 = h.sql("select min(amount ) from model_input_losswarning_t ") //最小购买金额
val min_amount    = result14.collect()(0).get(0).asInstanceOf[Integer].toDouble
val region_amount  = if((max_amount   - min_amount    ) == 0) 1 else (max_amount   - min_amount    )

val result15 = h.sql("select max(daily_pv ) from model_input_losswarning_t ")//最大日均访问量
val max_daily_pv = result15.collect()(0).get(0).asInstanceOf[Float].toDouble
val result16 = h.sql("select min(daily_pv ) from model_input_losswarning_t ") //最小日均访问量
val min_daily_pv = result16.collect()(0).get(0).asInstanceOf[Float].toDouble
val region_daily_pv  = if((max_daily_pv   - min_daily_pv    ) == 0) 1 else (max_daily_pv   - min_daily_pv    )

val result17 = h.sql("select max(item_page) from model_input_losswarning_t ")//最大商品页浏览占比
val max_item_page = result17.collect()(0).get(0).asInstanceOf[Float].toDouble
val result18 = h.sql("select min(item_page) from model_input_losswarning_t ") //最小商品页浏览占比
val min_item_page = result18.collect()(0).get(0).asInstanceOf[Float].toDouble
val region_item_page  = if((max_item_page - min_item_page ) == 0) 1 else (max_item_page - min_item_page )

val result19 = h.sql("select max(email_click_rate ) from model_input_losswarning_t ")//最大邮件点开率
val max_email_click_rate  = result19.collect()(0).get(0).asInstanceOf[Float].toDouble
val result20 = h.sql("select min(email_click_rate ) from model_input_losswarning_t ") //最小邮件点开率
val min_email_click_rate  = result20.collect()(0).get(0).asInstanceOf[Float].toDouble
val region_email_click_rate  = if((max_email_click_rate  - min_email_click_rate  ) == 0) 1 else (max_email_click_rate  - min_email_click_rate  )

val result21 = h.sql("select max(days_1month ) from model_input_losswarning_t ")//最大一个月访问占比
val max_days_1month   = result21.collect()(0).get(0).asInstanceOf[Float].toDouble
val result22 = h.sql("select min(days_1month ) from model_input_losswarning_t ") //最小一个月访问占比
val min_days_1month   = result22.collect()(0).get(0).asInstanceOf[Float].toDouble
val region_days_1month   = if((max_days_1month   - min_days_1month ) == 0) 1 else (max_days_1month   - min_days_1month )

val result23 = h.sql("select max(days_3month ) from model_input_losswarning_t ")//最大三个月访问占比
val max_days_3month    = result23.collect()(0).get(0).asInstanceOf[Float].toDouble
val result24 = h.sql("select min(days_3month ) from model_input_losswarning_t ") //最小三个月访问占比
val min_days_3month    = result24.collect()(0).get(0).asInstanceOf[Float].toDouble
val region_days_3month    = if((max_days_3month    - min_days_3month  ) == 0) 1 else (max_days_3month    - min_days_3month  )

val result25 = h.sql("select max(days_6month ) from model_input_losswarning_t ")//最大六个月访问占比
val max_days_6month    = result25.collect()(0).get(0).asInstanceOf[Float].toDouble
val result26 = h.sql("select min(days_6month ) from model_input_losswarning_t ") //最小六个月访问占比
val min_days_6month    = result26.collect()(0).get(0).asInstanceOf[Float].toDouble
val region_days_6month    = if((max_days_6month    - min_days_6month) == 0) 1 else (max_days_6month    - min_days_6month)


 val result27 = h.sql("select max(unix_timestamp(first_visit_time)) from model_input_losswarning_t")//最大首次访问
val max_first_visit_time = result27.collect()(0).get(0).asInstanceOf[Long].toDouble
val result28 = h.sql("select min(unix_timestamp(first_visit_time)) from model_input_losswarning_t") //最小首次访问
val min_first_visit_time = result28.collect()(0).get(0).asInstanceOf[Long].toDouble
val region_first_visit_time = if((max_first_visit_time - min_first_visit_time ) == 0) 1 else (max_first_visit_time - min_first_visit_time )

 val result29 = h.sql("select max(unix_timestamp(last_visit_time )) from model_input_losswarning_t")//最大首次访问
val max_last_visit_time  = result29.collect()(0).get(0).asInstanceOf[Long].toDouble
val result30 = h.sql("select min(unix_timestamp(last_visit_time )) from model_input_losswarning_t") //最小首次访问
val min_last_visit_time  = result30.collect()(0).get(0).asInstanceOf[Long].toDouble
val region_last_visit_time   = if((max_last_visit_time  - min_last_visit_time) == 0) 1 else (max_last_visit_time  - min_last_visit_time)

val io_normlization = h.sql("select t.lenovoid,t.vip,((t.consume_level-"+min_consume_level +")/"+region_consume_level+") as consume_level,((t.n_complain-"+min_n_complain +")/"+region_n_complain+") as n_complain,((t.n_refund-"+min_n_refund +")/"+region_n_refund+") as n_refund,((t.total_days-"+min_total_days +")/"+region_total_days+") as total_days,((t.total_time-"+min_total_time +")/"+region_total_time+") as total_time,((t.n_phurchase-"+min_n_phurchase +")/"+region_n_phurchase+") as n_phurchase,((t.amount-"+min_amount +")/"+region_amount+") as amount,((t.daily_pv-"+min_daily_pv+")/"+region_daily_pv+") as daily_pv,((t.item_page-"+min_item_page+")/"+region_item_page+") as item_page,((t.email_click_rate-"+min_email_click_rate+")/"+region_email_click_rate+") as email_click_rate,((t.days_1month-"+min_days_1month+")/"+region_days_1month+") as days_1month,((t.days_3month-"+min_days_3month+")/"+region_days_3month+") as days_3month,((t.days_6month-"+min_days_6month+")/"+region_days_6month+") as days_6month,((unix_timestamp(first_visit_time)-"+min_first_visit_time+")/"+region_first_visit_time+") as first_visit_time,((unix_timestamp(last_visit_time )-"+min_last_visit_time +")/"+region_last_visit_time +") as last_visit_time from model_input_losswarning_t t")

io_normlization.sort($"last_visit_time".desc).registerTempTable("tem_losswarning")
val up_sort = io_normlization.sort($"last_visit_time".desc)
val low_sort = io_normlization.sort($"last_visit_time")

val line = io_normlization.count

val up_5 = up_sort.limit((0.05 * line).toInt)
val low_5 = low_sort.limit((0.05 * line).toInt)

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
// val test_up_10000 = up_5.limit(10000)
// val test_low_10000 = low_5.limit(10000)

val up_test= up_5.rdd.map( s => Vectors.dense(1,s.get(2).toString.asInstanceOf[String].toDouble,s.get(3).toString.asInstanceOf[String].toDouble,s.get(4).toString.asInstanceOf[String].toDouble,s.get(5).toString.asInstanceOf[String].toDouble,s.get(6).toString.asInstanceOf[String].toDouble,s.get(7).toString.asInstanceOf[String].toDouble,s.get(8).toString.asInstanceOf[String].toDouble,s.get(9).toString.asInstanceOf[String].toDouble,s.get(10).toString.asInstanceOf[String].toDouble,s.get(11).toString.asInstanceOf[String].toDouble,s.get(12).toString.asInstanceOf[String].toDouble,s.get(13).toString.asInstanceOf[String].toDouble,s.get(14).toString.asInstanceOf[String].toDouble,s.get(15).toString.asInstanceOf[String].toDouble))

val low_test= low_5.rdd.map( s => Vectors.dense(0,s.get(2).toString.asInstanceOf[String].toDouble,s.get(3).toString.asInstanceOf[String].toDouble,s.get(4).toString.asInstanceOf[String].toDouble,s.get(5).toString.asInstanceOf[String].toDouble,s.get(6).toString.asInstanceOf[String].toDouble,s.get(7).toString.asInstanceOf[String].toDouble,s.get(8).toString.asInstanceOf[String].toDouble,s.get(9).toString.asInstanceOf[String].toDouble,s.get(10).toString.asInstanceOf[String].toDouble,s.get(11).toString.asInstanceOf[String].toDouble,s.get(12).toString.asInstanceOf[String].toDouble,s.get(13).toString.asInstanceOf[String].toDouble,s.get(14).toString.asInstanceOf[String].toDouble,s.get(15).toString.asInstanceOf[String].toDouble))


import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
// Load training data
val predicts = low_test.union(up_test)

val splits_up = up_test.sample(false,0.3, 11L)
val splits_low = low_test.sample(false,0.3, 11L)
 val splits_union = splits_up .union(splits_low )

//按spark机器学习实战的方法处理数据。
val parsedData = splits_union .map{  parts=>
LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1),parts(2),parts(3),parts(4),parts(5),parts(6),parts(7),parts(8),parts(9),parts(10),parts(11),parts(12),parts(13),parts(14)))
}.cache()

val prediction = predicts.map { parts =>
LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1),parts(2),parts(3),parts(4),parts(5),parts(6),parts(7),parts(8),parts(9),parts(10),parts(11),parts(12),parts(13),parts(14)))
}.cache()

val model = LogisticRegressionWithSGD.train(parsedData, 50)



val prediction_label = prediction .map {
case LabeledPoint (label, features) =>
val prediction = model.predict(features)
(prediction, label)
}

import org.apache.spark.mllib.evaluation.MulticlassMetrics

val metrics = new MulticlassMetrics(prediction_label)

val precision = metrics.precision

println("Precision = " + precision)