//归一化
val result = h.sql("select max(visit_times) from model_input_active_t")   //最大访问次数
val max_visit_times = result.collect()(0).get(0).asInstanceOf[Int].toDouble
val result = h.sql("select min(visit_times) from model_input_active_t")   //最小访问次数
val min_visit_times = result.collect()(0).get(0).asInstanceOf[Int].toDouble
val region_visit_times =if(( max_visit_times - min_visit_times) == 0) 1 else ( max_visit_times - min_visit_times)


val result = h.sql("select max(last_online_time) from model_input_active_t")   //最远登录天数
val max_last_online_time = result.collect()(0).get(0).asInstanceOf[Int].toDouble
val result = h.sql("select min(last_online_time) from model_input_active_t")   //最小登录天数
val min_last_online_time = result.collect()(0).get(0).asInstanceOf[Int].toDouble
val region_last_online_time =if(( max_last_online_time - min_last_online_time ) == 0) 1 else ( max_last_online_time - min_last_online_time)


val result = h.sql("select max(pay_times) from model_input_active_t")   //最大支付次数
val max_pay_times = result.collect()(0).get(0).asInstanceOf[Int].toDouble
val result = h.sql("select min(pay_times) from model_input_active_t")   //最小支付次数
val min_pay_times = result.collect()(0).get(0).asInstanceOf[Int].toDouble
val region_pay_times =if(( max_pay_times - min_pay_times ) == 0) 1 else (  max_pay_times - min_pay_times)

val result = h.sql("select max(comment_times) from model_input_active_t")   //最大问询评论数
val max_comment_times = result.collect()(0).get(0).asInstanceOf[Int].toDouble
val result = h.sql("select min(comment_times) from model_input_active_t")   //最小问询评论数
val min_comment_times = result.collect()(0).get(0).asInstanceOf[Int].toDouble
val region_comment_times =if(( max_comment_times - min_comment_times ) == 0) 1 else (  max_comment_times - min_comment_times)

val result = h.sql("select max(stay_time) from model_input_active_t")   //最大停留时间
val max_stay_time = result.collect()(0).get(0).asInstanceOf[Float].toDouble
val result = h.sql("select min(stay_time) from model_input_active_t")   //最小停留时间
val min_stay_time = result.collect()(0).get(0).asInstanceOf[Float].toDouble
val region_stay_time =if(( max_stay_time - min_stay_time ) == 0) 1 else (  max_stay_time - min_stay_time)


val result = h.sql("select max(visit_day_times) from model_input_active_t")   //最大登录天数
val max_visit_day_times = result.collect()(0).get(0).asInstanceOf[Int].toDouble
val result = h.sql("select min(visit_day_times) from model_input_active_t")   //最小登录天数
val min_visit_day_times = result.collect()(0).get(0).asInstanceOf[Int].toDouble
val region_visit_day_times =if(( max_visit_day_times - min_visit_day_times ) == 0) 1 else (   max_visit_day_times - min_visit_day_times)

//权重：visit_times:0.2,visit_targetpage_percen:0.1,last_online_time:0.1,pay_times:0.2,comment_times:0.2,stay_time:0.1,visit_day_times 0.1
val normalization= h.sql("select t1.cookie , ((t1.visit_times- "+min_visit_times+")*0.2/"+region_visit_times+") as visit_times, t1.visit_targetpage_percen*0.1, ((t1.last_online_time- "+min_last_online_time+")*0.1/"+region_last_online_time+") as last_online_time, ((t1.pay_times- "+min_pay_times+")*0.2/"+region_pay_times+") as pay_times, ((t1.comment_times- "+min_comment_times+")*0.2/"+region_comment_times+") as comment_times, ((t1.stay_time- "+min_stay_time+")*0.1/"+region_stay_time+") as stay_time, ((t1.visit_day_times- "+min_visit_day_times+")*0.1/"+region_visit_day_times+") as visit_day_times from model_input_active_t t1")


import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix


//DataFrame转化为Vectors，没发现直接的API，方案是Dataframe转为rdd，然后，调用Vectors.dense，把它们集合起来
val data = normalization.rdd.map(line => Vectors.dense(line.get(1).toString.asInstanceOf[String].toDouble,line.get(2).toString.asInstanceOf[String].toDouble,line.get(3).toString.asInstanceOf[String].toDouble,line.get(4).toString.asInstanceOf[String].toDouble,line.get(5).toString.asInstanceOf[String].toDouble,line.get(6).toString.asInstanceOf[String].toDouble,line.get(7).toString.asInstanceOf[String].toDouble))

val rm = new RowMatrix(data)

val pc = rm.computePrincipalComponents(1)
val mx = rm.multiply(pc)




