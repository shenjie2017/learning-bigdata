package com.blue.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shenjie
  * @version v1.0
  * @Description
  * @Date: Create in 17:20 2018/7/5
  * @Modifide By:
  **/

//      ┏┛ ┻━━━━━┛ ┻┓
//      ┃　　　　　　 ┃
//      ┃　　　━　　　┃
//      ┃　┳┛　  ┗┳　┃
//      ┃　　　　　　 ┃
//      ┃　　　┻　　　┃
//      ┃　　　　　　 ┃
//      ┗━┓　　　┏━━━┛
//        ┃　　　┃   神兽保佑
//        ┃　　　┃   代码无BUG！
//        ┃　　　┗━━━━━━━━━┓
//        ┃　　　　　　　    ┣┓
//        ┃　　　　         ┏┛
//        ┗━┓ ┓ ┏━━━┳ ┓ ┏━┛
//          ┃ ┫ ┫   ┃ ┫ ┫
//          ┗━┻━┛   ┗━┻━┛



object SQLDemo {

  case class Person(id:Long,name:String,age:Int,sex:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkSQL-Demo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val personRDD = sc.textFile("d://tmp/spark/person_in").map(t =>{
      val fields = t.split(",")
      Person(fields(0).toLong,fields(1),fields(2).toInt,fields(3))
    })

    import sqlContext.implicits._

    val personDF = personRDD.toDF
//    println("DataFrame:")
    personDF.select("id","name","age").filter(personDF("age")>=25).show()


//    println("sparksql:")
    personDF.registerTempTable("t_person")
    sqlContext.sql("select * from t_person where age>20").show()

    sc.stop()
  }
}


