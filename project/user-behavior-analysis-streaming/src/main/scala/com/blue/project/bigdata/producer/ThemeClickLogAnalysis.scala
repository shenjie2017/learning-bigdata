package com.blue.project.bigdata.producer

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ThemeClickLogAnalysis {
  var checkpoint_path:String = _

  def createStreamingContext() :StreamingContext = {
    val conf = new SparkConf().setAppName(getClass.getName)
    conf.set("spark.streaming.kafka.maxRetries", "100")
    conf.set("spark.streaming.kafka.maxRatePerParititon", "1000")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))
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
      "group.id"->"spark_streaming_save_hdfs",
      "auto.offset.reset"->"latest",
      "enable.auto.commit"->"true")

    val topics = Set(args(1))

    val kafkaDStream = KafkaUtils.createDirectStream[String, String]( ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaConf))

    val obj = kafkaDStream.map(_.value()).map(line=>{
//      user_id,theme_id,item_id,leaf_cate_id,cate_level1_id,clk_cnt,reach_time
      val arr = line.split(",")
      (arr(0),arr(1),arr(2),arr(3),arr(4),arr(5),arr(6))
    })
      obj.map(item=>(item._1,item._6.toInt)).reduceByKeyAndWindow((v1,v2)=>v1+v2,(v1,v2)=>v1-v2,Seconds(10),Seconds(2))
      .transform(rdd=>rdd.sortBy(_._2,false)).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
