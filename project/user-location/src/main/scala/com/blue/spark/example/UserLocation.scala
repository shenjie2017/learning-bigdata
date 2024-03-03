package com.blue.spark.example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shenjie
  * @version v1.0
  * @Description
  * @Date: Create in 11:31 2018/6/29
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

object UserLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //src:SCYD1000000001,104.082003,30.646366,新南路82号四凯大楼6层
    //new:Array((SCYD1000000001,104.082003,30.646366,新南路82号四凯大楼6层), (SCYD1000000002,104.076243,30.523343,地铁1号线四河地铁站), (SCYD1000000003,104.073066,30.639477,省体育馆地铁一号线))
    val rdd0 = sc.textFile("d://tmp/spark/location_in").map(line => {
      val fields = line.split(",")
      (fields(0),(fields(1),fields(2),fields(3)))
    })

//    src:18812345678,SCYD1000000001,1530207538000,1
//    new:(18812345678_SCYD1000000001,-1530207538000)
//    new:(18812345678_SCYD1000000001,1530211138000)
    val rdd1 = sc.textFile("d://tmp/spark/userinfo_in").map(line => {
      val fields = line.split(",")
      val mobile = fields(0)
      val lac = fields(1)
      val time = if("1" == fields(3)) -fields(2).toLong else fields(2).toLong

      (mobile + "_" + lac , time)
    })

//    new:Array((18812345678_SCYD1000000003,12000),(17712341234_SCYD1000000001,120000), (18812345678_SCYD1000000001,9612000))
    val rdd2 = rdd1.groupBy(_._1).mapValues(_.foldLeft(0L)(_ + _._2))

//    new:Array((18812345678,SCYD1000000003,12000), (17712341234,SCYD1000000001,120000))
    val rdd3 = rdd2.map(t => {
      val fields = t._1.split("_")
      val mobile = fields(0)
      val lac = fields(1)
      val time = t._2
      (mobile,lac,time)
    })

//    new:(17712341234,CompactBuffer((17712341234,SCYD1000000001,120000), (17712341234,SCYD1000000003,1795000)))
    val rdd4 = rdd3.groupBy(_._1)

//    new:(17712341234,List((17712341234,SCYD1000000003,1795000), (17712341234,SCYD1000000001,120000)))
    val rdd5 = rdd4.mapValues(it =>{
      it.toList.sortBy(_._3).reverse
    })
//    new:Array(List((15534343434,SCYD1000000001,800000)), List((17712341234,SCYD1000000003,1795000), (17712341234,SCYD1000000001,120000)))
    val rdd6 = rdd5.map(t=>{
      t._2
    })

    //new:(15534343434,SCYD1000000001,800000), (17712341234,SCYD1000000003,1795000)
    val rdd7 = rdd6.flatMap(x=>x)

    val rdd8 = rdd7.map(t=>{
      (t._2,(t._1,t._3))
    })

//    new:(SCYD1000000003,((104.073066,30.639477,省体育馆地铁一号线),(17712341234,1795000))), (SCYD1000000003,((104.073066,30.639477,省体育馆地铁一号线))
    val rdd9 = rdd0.join(rdd8)

//    new:(15534343434,SCYD1000000002,5000000,104.076243,30.523343,地铁1号线四河地铁站),(18812345678,SCYD1000000002,13600000,104.076243,30.523343,地铁1号线四河地铁站)
    val rdd10 = rdd9.map(t=>{
      val mobile = t._2._2._1
      val lac = t._1
      val time = t._2._2._2
      val lon = t._2._1._1
      val lat = t._2._1._2
      val address = t._2._1._3

      (mobile,lac,time,lon,lat,address)
    })

    rdd10.repartition(1).saveAsTextFile("d://tmp/spark/userinfo_out1")

    sc.stop()
  }
}
