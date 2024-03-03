package com.blue.spark.example


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Demo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext

    val rdd1 = sc.parallelize(Array((1,2),(3,4),(5,6)))
    rdd1.foreach(print)
    println()
    rdd1.mapValues(_*10).foreach(print)
    println()
    rdd1.flatMapValues(_ to 5).foreach(print)
    println()
    rdd1.flatMapValues(Array(_)).foreach(print)
    println()
    rdd1.map(item=> {
      val arr=new ArrayBuffer[(Int,Int)]()
      for (i <- item._2 to 5) {
        arr.append((item._1,i))
      }
      arr
    }).foreach(print)
    println()
    rdd1.map(item=> {
      val arr=new ArrayBuffer[(Int,Int)]()
      for (i <- item._2 to 5) {
        arr.append((item._1,i))
      }
      arr
    }).flatMap(item=>item).foreach(print)

    println()
    val rdd2 = sc.makeRDD(Array((1,"A"),(2,"B"),(3,"C"),(4,"D")),2)
    rdd2.flatMapValues(x => x + "_" + "*").foreach(print)
    println()
    rdd2.flatMapValues(x => Array(x,"_","*")).foreach(print)
  }
}
