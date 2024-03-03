package com.blue.spark

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author shenjie
  * @version v1.0
  * @Description
  * @Date: Create in 16:28 2018/7/6
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

object StateWordCount {

  val updateFunc = (iter:Iterator[(String, Seq[Int], Option[Int])]) => {
//      iter.map(t=>(t._1,t._2.sum+t._3.getOrElse(0)))
//      iter.flatMap(it=>Some(it._1,it._2.sum + it._3.getOrElse(0)).map(m =>(it._1,m)))
      iter.map{
        case(word,current_count,history_count)=>(word,current_count.sum + history_count.getOrElse(0))
      }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    sc.setCheckpointDir("d://tmp/spark/checkpoint")

    val ds = ssc.socketTextStream(args(0),args(1).toInt)
    val result = ds.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc,new HashPartitioner(sc.defaultParallelism),true)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
