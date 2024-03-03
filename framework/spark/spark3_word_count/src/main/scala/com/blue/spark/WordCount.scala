package com.blue.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.textFile("/opt/apache/maven-3.6.3/README.txt")
      .flatMap(_.split("\\W+")).filter(!_.isEmpty)
      .map((_,1))
      .groupBy(_._1)
      .map(item=> (item._1,item._2.count(_=>true)))
      .sortBy(_._2,ascending=false)
      .saveAsTextFile("/tmp/tmp_data.dat")

    val rdd1 = sc.parallelize(Seq(("1","zhangsan"),("2","lisi"),("3","wangwu")))
    val rdd2 = sc.parallelize(Seq(("1",29),("2",18),("3",36)))
    val rdd3 = rdd1.join(rdd2)
    rdd3.foreach(println)

    sc.stop()
  }
}
