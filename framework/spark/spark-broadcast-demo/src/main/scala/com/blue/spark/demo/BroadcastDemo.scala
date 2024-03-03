package com.blue.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Jason
  * @E-mail: 1075850619@qq.com
  * @Date: create in 2018/10/10 17:52
  * @Modified by:
  * @Project: learning-spark
  * @Package:
  * @Description:
  */
object BroadcastDemo {

  var c1 = 0
  var c2 = 0

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcostDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    c1 = 10

    val rdd1 = sc.parallelize(1 to 10, 2)
    val c2_init = 10
    c2 = sc.broadcast(c2_init).value
    val c3_init = 10
    var c3 = sc.broadcast(c3_init).value

    rdd1.mapPartitions(t => {
      System.out.println("before get c1:" + c1)
      System.out.println("before get c2:" + c2)
      System.out.println("before get c3:" + c3)
      t
    })
      .collect()
  }

}
