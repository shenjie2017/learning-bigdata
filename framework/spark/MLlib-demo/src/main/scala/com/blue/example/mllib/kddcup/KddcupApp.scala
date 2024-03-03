package com.blue.example.mllib.kddcup

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Jason
  * @E-mail: 1075850619@qq.com
  * @Date: create in 2019/4/26 14:14
  * @Modified by:
  * @Project: learning-spark
  * @Package: com.blue.example.mllib.kddcup
  * @Description: k-means聚类非监督学习-网络流量异常检测
  */
object KddcupApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass.getName)
    val sc = new SparkContext(conf)

    val rawData = sc.textFile(args(0))
    //        rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)
    val labelsAndData = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1, 3)
      val label = buffer.remove(buffer.length - 1)
      val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
      (label, vector)
    }
    val data = labelsAndData.values.cache()
    //    val kmeans = new KMeans()
    //    val model = kmeans.run(data)
    //    model.clusterCenters.foreach(println)

    //    val clusterLabelCount = labelsAndData.map { case (label, datum) =>
    //      val cluster = model.predict(datum)
    //      (cluster, label)
    //    }.countByValue
    //
    //    clusterLabelCount.toSeq.sorted.foreach { case ((cluster, label), count) =>
    //      println(f"$cluster%1s$label%18s$count%8s")
    //    }

    (30 to 100 by 10).map(k => (k, clusteringScore(data, k))).foreach(println)

  }

  def distance(a: Vector, b: Vector) = math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  def distToCentroid(datum: Vector, model: KMeansModel) = {
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
  }

  def clusteringScore(data: RDD[Vector], k: Int) = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    //    kmeans.setRuns(10)
    kmeans.setMaxIterations(200)
    kmeans.setEpsilon(1.0e-10)

    val model = kmeans.run(data)
    data.map(datum => distToCentroid(datum, model)).mean()
  }
}
