val input_df = hiveContext.sql("select t.lenovo_id,t.monetary,cast(t.frequency as int) as frequency,t.recency from model_input_rfm_t t")
val row_nums = input_df.count.toInt       //获得总行数
val row_partition = row_nums / 5      //获得5分区点
val row_partition6 = row_nums / 6    //获得6分区点
val input_sort_monetary = input_df.sort($"monetary".desc).collect()         
val input_sort_frequency = input_df.sort($"frequency".desc).collect()      //wrong
val input_sort_recency = input_df.sort($"recency".desc).collect()
//monetary的分区
val monetary_1 = input_sort_monetary(row_partition * 1).get(1).asInstanceOf[Number].intValue
val monetary_2 = input_sort_monetary(row_partition * 2).get(1).asInstanceOf[Number].intValue
val monetary_3 = input_sort_monetary(row_partition * 3).get(1).asInstanceOf[Number].intValue
val monetary_4 = input_sort_monetary(row_partition * 4).get(1).asInstanceOf[Number].intValue
//frequency的分区
val frequency_1 = input_sort_frequency (row_partition * 1).get(2).asInstanceOf[Integer].toInt
val frequency_2 = input_sort_frequency (row_partition * 2).get(2).asInstanceOf[Integer].toInt
val frequency_3 = input_sort_frequency (row_partition * 3).get(2).asInstanceOf[Integer].toInt
val frequency_4 = input_sort_frequency (row_partition * 4).get(2).asInstanceOf[Integer].toInt
//recency的分区
val result= input_sort_recency(row_partition6 * 1).get(3).asInstanceOf[String].toString
val recency_1 = result.substring(0,4)+result.substring(5,7)+result.substring(8,10)
val result= input_sort_recency(row_partition6 * 2).get(3).asInstanceOf[String].toString
val recency_2 = result.substring(0,4)+result.substring(5,7)+result.substring(8,10)
val result= input_sort_recency(row_partition6 * 3).get(3).asInstanceOf[String].toString
val recency_3 = result.substring(0,4)+result.substring(5,7)+result.substring(8,10)

val result= input_sort_recency(row_partition6 * 4).get(3).asInstanceOf[String].toString
val recency_4 = result.substring(0,4)+result.substring(5,7)+result.substring(8,10)
val result= input_sort_recency(row_partition6 * 5).get(3).asInstanceOf[String].toString
val recency_5 = result.substring(0,4)+result.substring(5,7)+result.substring(8,10)
val io_monetary = hiveContext.sql("(select t1.lenovo_id, t1.frequency, t1.monetary, t1.recency, t1.points, t1.flag, t1.cluster from model_output_rfm_t t1  where 1=0 ) union all (select t2.lenovo_id, t2.frequency, t2.monetary, t2.recency,(case when t2.monetary > "+monetary_1+ " then 5 when t2.monetary >"+monetary_2+" then 4 when t2.monetary > "+monetary_3+" then 3 when t2.monetary >"+monetary_4 + " then 2 else  1 end) as points, ' ', ' ' from model_input_rfm_t t2)")
io_monetary .registerTempTable("temporary_monetary")     //金额临时表
val io_frequency = hiveContext.sql("(select t1.lenovo_id, t1.frequency, t1.monetary, t1.recency, t1.points, t1.flag, t1.cluster from model_output_rfm_t t1  where 1=0 ) union all (select t2.lenovo_id, t2.frequency, t2.monetary, t2.recency,(case when t2.frequency> "+frequency_1 + " then (50+t3.points) when t2.frequency>"+frequency_2 +" then (40+t3.points) when t2.frequency> "+frequency_3 +" then (30+t3.points) when t2.frequency>"+frequency_4 + " then (20+t3.points) else  (10+t3.points) end) as points, ' ', ' ' from model_input_rfm_t t2,temporary_monetary t3 where t2.lenovo_id = t3.lenovo_id)")
io_frequency.registerTempTable("temporary_frequency")     //频率临时表
//归一化
val result = hiveContext.sql("select max(cast(frequency as int)) from model_input_rfm_t")   //求最大频率
val max_frequency = result.collect()(0).get(0).asInstanceOf[Integer].toInt
val result = hiveContext.sql("select min(cast(frequency as int)) from temporary_frequency")   //最小频率
val min_frequency = result.collect()(0).get(0).asInstanceOf[Integer].toInt
val region_frequency = max_frequency - min_frequency 
val result = hiveContext.sql("select max(unix_timestamp(concat(substring(t2.recency,0,4),substring(t2.recency,6,2),substring(t2.recency,9,2)),'yyyyMMdd')) from  temporary_frequency t2")
val max_recency = result.collect()(0).get(0).asInstanceOf[Long]         //最大时间
val result = hiveContext.sql("select min(unix_timestamp(concat(substring(t2.recency,0,4),substring(t2.recency,6,2),substring(t2.recency,9,2)),'yyyyMMdd')) from  temporary_frequency t2")
val min_recency = result.collect()(0).get(0).asInstanceOf[Long]     //最小时间
val region_recency = max_recency - min_recency                                   //时间最大区间
val result =hiveContext.sql("select max(monetary) from model_input_rfm_t")
val max_monetary =  result.collect()(0).get(0).asInstanceOf[Float]              //最大金额
//val result =hiveContext.sql("select min(monetary) from model_input_rfm_t")
//val min_monetary =  result.collect()(0).get(0).asInstanceOf[Float]              //最小金额
val min_monetary = 0
val region_monetary = max_monetary - min_monetary                //金额最大区间
val io_recency = hiveContext.sql("(select t1.lenovo_id, t1.frequency, t1.monetary, t1.recency, t1.points, t1.flag, t1.cluster from model_output_rfm_t t1 where 1=0 ) union all (select t2.lenovo_id, ((t2.frequency - "+min_frequency+")/" + region_frequency + ") as frequency, ((t2.monetary - "+min_monetary+") /" + region_monetary+") as monetary, ((unix_timestamp(t2.recency,'yyyy-MM-dd')- "+min_recency+") / " + region_recency + ") as recency,(case when concat(substring(t2.recency,0,4),substring(t2.recency,6,2),substring(t2.recency,9,2))> "+recency_1+ " then (600+t3.points) when concat(substring(t2.recency,0,4),substring(t2.recency,6,2),substring(t2.recency,9,2))>"+recency_2+" then (500+t3.points) when concat(substring(t2.recency,0,4),substring(t2.recency,6,2),substring(t2.recency,9,2))> "+recency_3+" then (400+t3.points) when concat(substring(t2.recency,0,4),substring(t2.recency,6,2),substring(t2.recency,9,2))>"+recency_4+ " then (300+t3.points)   when concat(substring(t2.recency,0,4),substring(t2.recency,6,2),substring(t2.recency,9,2))>"+recency_5+ " then (200+t3.points) else  (100+t3.points) end) as points, ' ', ' ' from model_input_rfm_t t2,temporary_frequency t3 where t2.lenovo_id = t3.lenovo_id)")
io_recency.registerTempTable("temporary_recency")     //日期临时表

//聚类算法
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
//DataFrame转化为RDD，直接io_recency.rdd即可
val parsedData =  io_recency.rdd.map( s => Vectors.dense(s.get(1).asInstanceOf[String].toDouble,s.get(2).asInstanceOf[Double],s.get(3).asInstanceOf[String].toDouble))      //.cache()
val numClusters = 8
val numIterations = 20
val model = KMeans.train(parsedData, numClusters, numIterations)
model.clusterCenters.foreach(println)
val WSSSE = model.computeCost(parsedData)
println("Within Set Sum of Squared Errors = " + WSSSE)
val insertData =  io_recency.rdd.map( s => Vectors.dense(s.get(0).asInstanceOf[String].toLong,s.get(1).asInstanceOf[String].toDouble,s.get(2).asInstanceOf[Double],s.get(3).asInstanceOf[String].toDouble,s.get(4).asInstanceOf[Integer].toInt,' ',model.predict(Vectors.dense(s.get(1).asInstanceOf[String].toDouble,s.get(2).asInstanceOf[Double],s.get(3).asInstanceOf[String].toDouble))) )  //.cache()

import spark.implicits._
case class Cluster(lenovo_id: Long, frequency:Double,monetary:Double,recency:Double,points:Double,flag:Double,cluster:Double)
val rdd_df = insertData.map(attributes => Cluster(attributes(0).toLong, attributes(1).toDouble, attributes(2).toDouble, attributes(3).toDouble, attributes(4).toDouble, attributes(5).toDouble, attributes(6).toDouble)).toDF()
rdd_df.registerTempTable("temporary_cluster")
hiveContext.sql("insert overwrite table userfigure_local.model_output_rfm_t  partition (l_day='2016-10-01') select * from temporary_cluster")
val io_cluster = hiveContext.sql("(select t1.lenovo_id, t1.frequency, t1.monetary, t1.recency, t1.points, t1.flag, t1.cluster from model_output_rfm_t t1where 1=0 ) union all (select t2.lenovo_id, t2.frequency, t2.monetary, t2.recency,t2.points, t2.flag,t2.cluster from temporary_cluster t2)")
hiveContext.sql("insert into model_output_rfm_t partition(l_day='2016-10-01') select * from table1")
