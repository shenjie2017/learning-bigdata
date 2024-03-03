package com.blue.project.vehicle.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Created by root on 2016/5/24.
  */
object JedisPool{

  val host = "192.168.163.111"
  val port = 6379

  val config = new JedisPoolConfig()
  //最大连接数,
  config.setMaxTotal(10)
  //最大空闲连接数,
  config.setMaxIdle(5)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  val pool = new JedisPool(config, host, port)

  def getConnection(): Jedis = {
    pool.getResource

  }

  def main(args: Array[String]) {
    val conn = JedisPool.getConnection()
    val result = conn.ping()
    println(result)
    val r = conn.keys("*")
    println(r)
  }










}
