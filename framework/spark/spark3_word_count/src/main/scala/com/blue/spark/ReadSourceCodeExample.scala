package com.blue.spark

import org.apache.spark.{SparkConf, SparkContext}

object ReadSourceCodeExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)



    sc.stop()
  }
}
