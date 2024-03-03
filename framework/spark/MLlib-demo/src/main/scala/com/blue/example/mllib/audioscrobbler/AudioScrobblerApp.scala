package com.blue.example.mllib.audioscrobbler

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Jason
  * @E-mail: 1075850619@qq.com
  * @Date: create in 2019/4/24 10:37
  * @Modified by: 
  * @Project: learning-spark
  * @Package: com.blue.example.mllib.audioscrobbler
  * @Description: 最小二乘法(ALS)推荐系统
  */

object AudioScrobblerApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    val rawUserArtistData = sc.textFile(args(0))

    val rawArtistData = sc.textFile(args(1))
    val artistById = rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case e: NumberFormatException => None
        }
      }
    }

    val rawArtistAlias = sc.textFile(args(2))
    val artistAlias = rawArtistAlias.flatMap { line =>
      val tokens = line.split('\t')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(1).toInt))
      }
    }.collectAsMap()

    val bArtistAlias = sc.broadcast(artistAlias)
    val trainData = rawUserArtistData.map { line =>
      val Array(userId, artistId, count) = line.split(' ').map(_.toInt)
      //get artist real name for artistId
      val finalArtistId = bArtistAlias.value.getOrElse(artistId, artistId)
      Rating(userId, finalArtistId, count)
    }.cache()

    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
//    println(model.userFeatures.mapValues(_.mkString(", ")).first())

    val rawArtistForUser = rawUserArtistData.map(_.split(' ')).filter{case Array(userId,_,_) => userId.toInt==2093760}
    val existingProducts = rawArtistForUser.map{case Array(_,artistId,_) => artistId.toInt}.collect().toSet
    artistById.filter{case(id,name)=> existingProducts.contains(id)}.values.collect().foreach(println)

    val recommendations = model.recommendProducts(2093760,5)
    recommendations.foreach(println)
  }
}
