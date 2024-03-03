package com.blue.project.vehicle.report

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author shenjie
  * @version v1.0
  * @Description
  * @Date: Create in 17:33 2018/8/3
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


object VehicleSQLCount {

  case class GpsInfo(val g_vno:String,val g_date:String,val g_time:String,val g_lon:String,val g_lat:String
                    ,val g_mile:Double,val g_speed:Int,val g_direction:String,val g_address:String,val g_status:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("VehicleSQLCount").setMaster("local[5]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val gpsInfo = sc.textFile("D:\\tmp\\logs\\gps\\in\\gps.dat").map(record=>{
      val fields = record.split("\t")
      GpsInfo(fields(0),fields(1).split(" ")(0),fields(1).split(" ")(1),fields(2),fields(3),
        fields(4).toDouble,fields(5).toInt,fields(6),fields(7),fields(8))
    })

    import sqlContext.implicits._

    val gpsDF = gpsInfo.toDF()
    gpsDF.select("g_vno","g_date","g_time","g_speed","g_address").filter(gpsDF("g_speed")>80).show()

    gpsDF.registerTempTable("t_gps")
    sqlContext.sql("select * from t_gps where g_speed <100 and g_speed>60").show()

    sc.stop()
  }
}
