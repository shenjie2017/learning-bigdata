package com.blue.project.vehicle.report

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shenjie
  * @version v1.0
  * @Description
  * @Date: Create in 15:06 2018/8/3
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

class GpsInfo(val _date:String,val _time:String)
  extends Ordered[GpsInfo] with Serializable{
  override def compare(that: GpsInfo) : Int = {
    val time1 = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").parse(this._date+" "+this._time).getTime
    val time2 = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").parse(that._date+" "+that._time).getTime

    (time1-time2).toInt
  }
}

object DriverMileCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DriverMileCount").setMaster("local[5]")
    val sc = new SparkContext(conf)

    val rdd_hdfs = sc.textFile("D:\\tmp\\logs\\gps\\in\\gps.dat")

    val rdd_filter = rdd_hdfs.filter(_.split("\t").length==9)

      val rdd_tuple = rdd_filter.map(record => {
      val fields =  record.split("\t")
      (fields(0),fields(1).split(" ")(0),fields(1).split(" ")(1),fields(2).toDouble
        ,fields(3).toDouble,fields(4).toDouble,fields(5).toInt
        ,fields(6),fields(7),fields(8))
    })

    val rdd_group = rdd_tuple.groupBy(t=>{
      t._1+"_"+t._2
    })

    val rdd_sort = rdd_group.mapValues(iter=>{
      iter.toList.sortBy(t=> new GpsInfo(t._2,t._3))
    })

    val rdd_result = rdd_sort.mapValues(iter=>{
      var key:String = null
      var miles:Double = 0
      var lastMile:Double = -1
      iter.map(t=>{
        if(null==key){
          key = t._1+"_"+t._2
        }
        if(-1==lastMile){
          lastMile = t._6
        }else if(lastMile<t._6){
          miles = miles + (t._6-lastMile)
        }
      })
      (key,miles)
    }).map(_._2)

    rdd_result.collect()
    rdd_result.repartition(1).saveAsTextFile("D:\\tmp\\logs\\gps\\out")
    sc.stop()
  }
}
