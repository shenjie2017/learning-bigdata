package com.blue.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shenjie
 * @version v1.0
 * @Description
 * @Date: Create in 17:09 2018/6/27
 * @Modifide By:
 **/

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.textFile("/opt/apache/maven-3.6.3/README.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _,1).sortBy(_._2,false).foreach(println)

    sc.stop()
  }
}
