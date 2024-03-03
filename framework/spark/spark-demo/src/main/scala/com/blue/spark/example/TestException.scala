package com.blue.spark.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestException {
  def div(num:Int,base:Int):Int={
    num/base
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext

    val rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9))

    rdd.map(num=>{
      try {
        1 / 0
      }catch {
        case ex:Exception=>println(s"num:${num}. cause exception1")
      }

      try {
        div(num, 0)
      }catch {
        case ex:Exception=>println(s"num:${num}. cause exception2")
      }

      num
    }).collect()

    sc.stop()
  }
}
