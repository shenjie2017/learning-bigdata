package com.blue.example.mllib.covtype

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Jason
  * @E-mail: 1075850619@qq.com
  * @Date: create in 2019/4/25 14:13
  * @Modified by: 
  * @Project: learning-spark
  * @Package: com.blue.example.mllib.covtype
  * @Description: 随机树预测森林植被
  */
object CovtypeApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass.getName)
    val sc = new SparkContext(conf)

    val rawData = sc.textFile(args(0))
    val data = rawData.map { line =>
      val values = line.split(",").map(_.toDouble)
      val featureVector = Vectors.dense(values.init)
      val label = values.last - 1
      LabeledPoint(label, featureVector)
    }

    val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()

    val model = DecisionTree.trainClassifier(trainData, 7, Map[Int, Int](), "gini", 4, 100)
    val metrics = getMetrics(model, cvData)
    //    println(metrics.confusionMatrix)
    //    (0 until 7).map(cat => (metrics.precision(cat), metrics.recall(cat))).foreach(println)
    val trainPriorProbabilities = classProbabilities(trainData)
    val cvPriorProbabilities = classProbabilities(cvData)
    println(trainPriorProbabilities.zip(cvPriorProbabilities).map { case (trainProb, cvProb) => trainProb * cvProb }.sum)
  }

  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
    val predictionsAndLabels = data.map { example =>
      (model.predict(example.features), example.label)
    }
    new MulticlassMetrics(predictionsAndLabels)
  }

  def classProbabilities(data: RDD[LabeledPoint]): Array[Double] = {
    val countsByCategory = data.map(_.label).countByValue()
    val counts = countsByCategory.toArray.sortBy(_._1).map(_._2)
    counts.map(_.toDouble / counts.sum)
  }
}
