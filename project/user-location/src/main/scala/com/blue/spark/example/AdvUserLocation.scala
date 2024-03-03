package com.blue.spark.example

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * @author shenjie
  * @version v1.0
  * @Description
  * @Date: Create in 15:15 2018/6/29
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

object AdvUserLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AdvUserLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd0 = sc.textFile("d://tmp/spark/location_in").map(line=>{
      val fields = line.split(",")
      val lac = fields(0)
      val lon = fields(1)
      val lat = fields(2)
      val address = fields(3)

      (lac,(lon,lat,address))
    })

    val rdd1 = sc.textFile("d://tmp/spark/userinfo_in").map(line=>{
      val fields = line.split(",")

      val mobile = fields(0)
      val lac = fields(1)
      val time = if("1" == fields(3)) -fields(2).toLong else fields(2).toLong

      ((mobile , lac) , time)
    })

    val rdd2 = rdd1.reduceByKey(_ + _).map(t=>{
      val mobile = t._1._1
      val lac = t._1._2
      val time = t._2

      (lac,(mobile,time))
    })

    //(17712341234,SCYD1000000003,1795000,104.073066,30.639477,省体育馆地铁一号线)
    val rdd3 = rdd0.join(rdd2).map(t=>{
      val mobile = t._2._2._1
      val lac = t._1
      val time = t._2._2._2
      val lon = t._2._1._1
      val lat = t._2._1._2
      val address = t._2._1._3
      (mobile,lac,time,lon,lat,address)
    })

    val rdd4 = rdd3.groupBy(_._1)

    val rdd5 = rdd4.mapValues(it=>{
      it.toList.sortBy(_._3).reverse
    })

    rdd5.repartition(1).saveAsTextFile("d://tmp/spark/userinfo_out")

    sc.stop()

  }
}
