package com.blue.project.bigdata

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

case class UserBehaviorClass(userId: String, itemId: String, CategoryId: String, behaviorType: String, timestamp: Long)

object UserBehaviorAnalysis {

  def main(args: Array[String]): Unit = {
    //设置日志级别，避免输出大量无关的信息
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName(getClass.getName)
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val session_df1 = spark.read.format("csv").option("header", "false").option("inferSchema", "false").load(args(0))
      .toDF("userId", "itemId", "CategoryId", "behaviorType", "timestamp")
      .withColumn("ymd", from_unixtime($"timestamp", "yyyyMMdd"))
      .where($"ymd">="20171125" && $"ymd"<="20171203")
    session_df1.cache()
    //    session_df1.show(10)
    //    session_df1.printSchema()
    //    println(session_df1.rdd.toDebugString)

    //    val context_rdd1 = sc.textFile(args(0)).map(line => line.split(","))
    //        .map(fields => (fields(0),fields(1),fields(2),fields(3),fields(4).toLong))
    //    val context_df1 = rdd1.toDF("userId","itemId","CategoryId","behaviorType","timestamp")
    //    context_df1.show(10)
    //    context_df1.printSchema()
    //    println(context_df1.rdd.toDebugString)

    //    val dfAggr1 = session_df1.select("userId","itemId","CategoryId","behaviorType").groupBy("userId","behaviorType").count().
    //      withColumnRenamed("count","cnt")
    //    val window1 = Window.partitionBy("behaviorType").orderBy($"cnt".desc)
    //    val dfTop5 = dfAggr1.withColumn("rn",row_number().over(window1)).where($"rn"<=5).drop("rn").select("behaviorType","cnt","userId")
    //    dfTop5.show()

    val uv = session_df1.select("userId").agg(countDistinct("userId")).first().getLong(0)
    val pv = session_df1.select("userId")
      .where($"behaviorType" === "pv")
      .groupBy("userId").agg(count("*").as("cnt")).agg(sum("cnt").as("cnt")).first().getLong(0)
    //uv:987994 pv:89716264 pv/uv:90.8064866790689
    println("uv:" + uv + " pv:" + pv + " pv/uv:" + pv.toDouble / uv)
    //visit one times
    val ouv = session_df1.select("userId").groupBy("userId").agg(count("*").as("cnt"))
      .where($"cnt" === 1).agg(count("*").as("cnt")).first().getLong(0)
    //ouv:85 uv:987994 ouv/uv:8.603291113103926E-5
    println("ouv:" + ouv + " uv:" + uv + " ouv/uv:" + ouv.toDouble/uv)

    //用户总行为数漏斗
    val user_behaivor = session_df1.select("behaviorType").groupBy("behaviorType").agg(count("*").as("cnt")).orderBy($"cnt".desc)
    user_behaivor.show()

    //独立访客漏斗
    val bau = session_df1.select("userId","behaviorType").groupBy("behaviorType").agg(countDistinct("userId").as("cnt")).orderBy($"cnt".desc)
    bau.show()

    val date_user_behavior = session_df1.select("behaviorType","ymd","timestamp")
//      .withColumn("ymd", from_unixtime($"timestamp", "yyyyMMdd"))
//      .withColumn("year", from_unixtime($"timestamp", "yyyy"))
//      .withColumn("month", from_unixtime($"timestamp", "MM"))
//      .withColumn("day", from_unixtime($"timestamp", "dd"))
//      .withColumn("hour", from_unixtime($"timestamp", "HH"))
//      .where($"ymd">="20171125" && $"ymd"<="20171203")
//      .rollup("hour")
//      .rollup("year","month","day","hour")
//      .groupBy("hour")
      .groupBy("ymd")
      .agg(count(when($"behaviorType"==="pv","1").otherwise(null)).as("pv_cnt"),
        count(when($"behaviorType"==="cart","1")).as("cart_cnt"),
        count(when($"behaviorType"==="cart","1")).as("fav_cnt"),
        count(when($"behaviorType"==="buy","1")).as("buy_cnt"))
//      .withColumn("year",when($"year".isNull,"all").otherwise($"year"))
//      .withColumn("month",when($"month".isNull,"all").otherwise($"month"))
//      .withColumn("day",when($"day".isNull,"all").otherwise($"day"))
//      .withColumn("hour",when($"hour".isNull,"all").otherwise($"hour"))
//        .orderBy($"year".asc,$"month".asc,$"day".asc,$"hour".asc)
//        .orderBy($"hour".asc)
        .orderBy($"ymd".asc)
    date_user_behavior.show(1000)

    val itemTop10 = session_df1.select("behaviorType","itemId").where($"behaviorType"==="buy")
      .groupBy("itemId").agg(count("*").as("cnt")).orderBy($"cnt".desc)
    itemTop10.show(10)

    val itemPopTop10 = session_df1.select("behaviorType","itemId")
      .groupBy("itemId")
      .agg(count(when($"behaviorType"==="pv",1)).as("pv_cnt"),
        count(when($"behaviorType"==="buy",1)).as("buy_cnt"))
      .withColumn("rate",$"buy_cnt"/$"pv_cnt").where($"buy_cnt">=10 && $"buy_cnt"<$"pv_cnt").orderBy($"rate".desc)
    itemPopTop10.show(10)

    sc.stop()
  }
}
