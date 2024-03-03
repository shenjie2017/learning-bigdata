package com.blue.project.bigdata.producer

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ThemeClickLogConsumer {
  var checkpoint_path:String = _

  def createStreamingContext() :StreamingContext = {
    val conf = new SparkConf().setAppName(getClass.getName)
    conf.set("spark.streaming.kafka.maxRetries", "100")
    conf.set("spark.streaming.kafka.maxRatePerParititon", "1000")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint(checkpoint_path)
    ssc
  }

  def main(args: Array[String]): Unit = {
    checkpoint_path=args(0)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

    val ssc = StreamingContext.getOrCreate(checkpoint_path,createStreamingContext)

    val kafkaConf = Map[String,String]("bootstrap.servers"->"localhost:9092",
      "key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer",
     "group.id"->"spark_streaming_save_hdfs")

    val topics = Set(args(2))

    val kafka = KafkaUtils.createDirectStream[String, String]( ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaConf))

    kafka.map(_.value()).map(line=>{
//      user_id,theme_id,item_id,leaf_cate_id,cate_level1_id,clk_cnt,reach_time
      val arr = line.split(",")
      new StringBuffer().append(arr(0)).append(",")
        .append(arr(1)).append(",")
        .append(arr(2)).append(",")
        .append(arr(3)).append(",")
        .append(arr(4)).append(",")
        .append(arr(5)).append(",")
        .append(arr(6)).toString
    }).foreachRDD(rdd=>{
//      rdd.foreach(println)
      val datestr = args(1)+"/"+new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
      println(datestr)
      rdd.saveAsTextFile(datestr)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
