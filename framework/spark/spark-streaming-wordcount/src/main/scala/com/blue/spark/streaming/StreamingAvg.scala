package com.blue.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

object StreamingAvg {

  def updateFunc(new_values:Seq[Double], last_state:Option[Double]): Option[Double] = {
    print(new_values.length)
    val last_avg_value = last_state.getOrElse(6.0)
    var count_value=0
    var sum_value = 0.0
    for (value <- new_values){
      val check_value = value/last_avg_value
      if(check_value >=0.5 && check_value<=1.5 ){
        count_value += 1
        sum_value += value
      }
    }
    var new_avg_value=last_avg_value
    if(count_value!=0) new_avg_value=(sum_value/count_value + last_avg_value)/2
    Some(new_avg_value)
}

  def createContext(ip: String, port: Int, checkpointDirectory: String) : StreamingContext = {
    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context")
    val sparkConf = new SparkConf().setAppName(StreamingWC.getClass.getName)
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint(checkpointDirectory)

    val ds = ssc.socketTextStream(ip,port)
    val result = ds.flatMap(_.split(" ")).map(x=>(1,x.toDouble)).updateStateByKey(updateFunc=updateFunc)
    result.print()
    ssc
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val checkpointDirectory = "/tmp/checkpoint/spark-streaming"
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, () => createContext("127.0.0.1", 9999, checkpointDirectory))

    ssc.start()
    ssc.awaitTermination()
  }
}
