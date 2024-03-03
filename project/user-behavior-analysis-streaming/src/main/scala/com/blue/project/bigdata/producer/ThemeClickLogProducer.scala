package com.blue.project.bigdata.producer

import java.io.File
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source
import scala.util.Random

object ThemeClickLogProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers","localhost:9092")

    val producer = new KafkaProducer[String,String](props,new StringSerializer(),new StringSerializer())
    Source.fromFile(new File(args(0))).getLines().foreach(line=> {
//      println(line)
      val record = new ProducerRecord[String,String](args(1),line)
      Thread.sleep(new Random().nextInt(5))
      producer.send(record)
    })
  }
}
