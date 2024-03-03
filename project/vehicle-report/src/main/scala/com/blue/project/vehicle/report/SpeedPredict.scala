package com.blue.project.vehicle.report

import com.blue.project.vehicle.utils.JedisPool
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author shenjie
  * @version v1.0
  * @Description
  * @Date: Create in 16:50 2018/8/2
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
//        ┃　　　　　     　　       ┣┓
//        ┃　　　　                  ┏┛
//        ┗━┓ ┓ ┏━━━┳ ┓ ┏━┛
//          ┃ ┫ ┫   ┃ ┫ ┫
//          ┗━┻━┛   ┗━┻━┛

object SpeedPredict {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CountMsg").setMaster("local[5]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    sc.setCheckpointDir("D:\\tmp\\logs\\checkpoint")

//    val kafkaParams = Map("metadata.broker.list" ->  "192.168.163.111:9092,192.168.163.112:9092,192.168.163.113:9092",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean),
//      "group.id" -> "countMsg")

    val zkQuorum = "192.168.163.111:2181"
    val group = "countMsg"
    val topics = Map("gpslog"-> 1)

    val stream = KafkaUtils.createStream(ssc, zkQuorum, group, topics, StorageLevel.MEMORY_AND_DISK_SER)

//    print(stream.count())
    val rdd = stream.map(_._2)

    rdd.foreachRDD(rdd=>{
      rdd.foreachPartition(partitionOfRecords=>{
        val jedis = JedisPool.getConnection()
        partitionOfRecords.foreach(record=>{
          var fields = record.split("\t")
          if(fields.length == 9){
            var key = fields(0)
            var vehicleSpeed = fields(5).toInt

            var vno = key
            key = "max_speed_"+key
            var value = jedis.get(key)
            if(null==value){
              value = jedis.get("max_speed_default")
            }
            var warnSpeed = value.toInt

            if(vehicleSpeed > warnSpeed){
              key = "pass_speed_warn_" + vno
              jedis.lpush(key,record.toString)
            }
          }

        })
      })
    })

    rdd.count()

    ssc.start()

    ssc.awaitTermination()
  }
}
