package com.blue.spark.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestDFUnion {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext

    import sparkSession.implicits._


    val rdd = sc.parallelize(Array(("1","up","down","msg content1"),("2","up","down","msg content2"),("3","up","down","msg content3"),("1","up","down","msg content1")))
    rdd.foreach(println)

    val df1= rdd.toDF("col1","col2","col3","col4")

    df1.union(df1).show()

    val df2 = df1.withColumnRenamed("col2","flag")
    val df3 = df1.withColumnRenamed("col3","flag")
    val df4 = df2.union(df3)
    df4.show()

    val df5 = df1.withColumnRenamed("col2","flag").drop("col3")
    val df6 = df1.withColumnRenamed("col3","flag").drop("col2")
    val df7 = df5.union(df6)
    df7.show()

    sc.stop()
  }
}
