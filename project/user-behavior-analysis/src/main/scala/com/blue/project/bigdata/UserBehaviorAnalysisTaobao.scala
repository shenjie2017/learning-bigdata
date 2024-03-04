package com.blue.project.bigdata

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UserBehaviorAnalysisTaobao {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName(getClass().getName()).setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkContext = sparkSession.sparkContext
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    //user_id,theme_id,item_id,leaf_cate_id,cate_level1_id,clk_cnt,reach_time
    val click_log_df = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "false").load(args(0))
    //item_id,emb
    val item_embedding_df = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "false").load(args(1))
    //theme_id,item_id
    val theme_item_pool_df = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "false").load(args(2))
    //user_id,emb
    val user_embedding_df = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "false").load(args(3))
    //user_id,item_id,paytime
    val user_item_purchase_log_df = sparkSession.read.format("csv").option("header", "true").option("inferSchema", "false").load(args(4))
    click_log_df.cache()
    item_embedding_df.cache()
    theme_item_pool_df.cache()
    user_embedding_df.cache()
    user_item_purchase_log_df.cache()

//    click_log_df.select("user_id","theme_id","item_id","clk_cnt").join(user_embedding_df.select("user_id","emb"),"user_id").show(10)
//    click_log_df.join(user_embedding_df.withColumnRenamed("user_id","userid"),$"user_id"===$"userid","inner").show(10)
//    click_log_df.join(click_log_df,Seq("user_id","item_id"),"inner").show(10)
//    click_log_df.join(click_log_df.withColumnRenamed("user_id","userid"),
//      $"user_id"===$"userid" && click_log_df("item_id")===click_log_df.withColumnRenamed("user_id","userid")("item_id"),"inner").show(10)
//    click_log_df.join(click_log_df.limit(5),Seq("user_id","item_id"),"left").show(10)
//    click_log_df.join(user_embedding_df,click_log_df("user_id")===user_embedding_df("user_id"),"left").show(10)
//    click_log_df.join(user_embedding_df,click_log_df("user_id")===user_embedding_df("user_id"),"left").where(user_embedding_df("user_id").isNull).show(10)

//    item_embedding_df.show(10)
//    theme_item_pool_df.show(10)
//    user_embedding_df.show(10)
//    user_item_purchase_log_df.show(100)
//    click_log_df.withColumn("col1",expr("1")).join(click_log_df.withColumn("col1",expr("1")),"col1")
//      .rdd.foreach(println)
//    click_log_df.withColumn("col1",expr("1")).join(click_log_df.withColumn("col1",expr("1")),"col1").show(1000000)

//    val item_embedding_rdd = item_embedding_df.rdd.map(row=> (row(0),row(1)))
//    val theme_item_pool_rdd = theme_item_pool_df.rdd.map(row=> (row(1),row(0)))
//    println("11")
//    theme_item_pool_rdd.join(item_embedding_rdd).take(10).foreach(println)
//    println("22")
//    theme_item_pool_rdd.filter(v=>null==v._1).take(10).foreach(println)
//    println("33")
//    theme_item_pool_rdd.leftOuterJoin(item_embedding_rdd).filter(v => null==v._1 || ""==v._1).take(10).foreach(println)
//    println("44")
//    theme_item_pool_rdd.leftOuterJoin(item_embedding_rdd).filter(v => null!=v._1 && None.equals(v._2._2)).take(10).foreach(println)
//    println("55")
//    theme_item_pool_rdd.leftOuterJoin(item_embedding_rdd).filter(v => null!=v._1 && !None.equals(v._2._2)).take(10).foreach(println)

    user_item_purchase_log_df.show(10)

//    user_item_purchase_log_df.rdd.map(row => (row(0), row(1), row(2)) ).take(10).foreach(println(_))
    user_item_purchase_log_df.rdd
      .map(row => (row(0).toString,row(1),row(2).toString))
      //user_id降序,paytime升序
      .map(item=>( new SecondSortBy(item._1,item._3),item)).sortBy(item => item._1,false).map(_._2).take(10).foreach(println)

//    user_item_purchase_log_df.rdd.map(row => (row(0).toString, row(1).toString, row(2).toString) ).map(item=>(item._1,(item._2,item._3)))
//      .sortBy(_._2._2,false).groupByKey().sortByKey().take(10).foreach(println)
    user_item_purchase_log_df.rdd.map(row => (row(0).toString, row(1).toString, row(2).toString) ).map(item=>(item._1,(item._2,item._3)))
      .groupByKey().mapValues(iter=>iter.toList.sortBy(_._2).reverse).sortByKey().take(10).foreach(println)

    println(user_item_purchase_log_df.rdd.map(row => (row(0).toString, row(1).toString, row(2).toString) ).map(item=> (item._1,1))
      .reduceByKey(_+_).reduce((v1,v2)=>("all_records",v1._2+v2._2)))

    println("distinct_item_id:",user_item_purchase_log_df.rdd.map(row => (row(0).toString, row(1).toString, row(2).toString) )
      .groupBy(_._1).count())

    sparkContext.stop()
  }

  class SecondSortBy(val first:String,val second:String) extends Ordered[SecondSortBy] with Serializable {
    override def compare(that: SecondSortBy): Int = {
      if(0!= first.compareTo(that.first)){
        return first.compareTo(that.first) //asc
      }else{
        return that.second.compareTo(second)//desc
      }
    }
  }

}
