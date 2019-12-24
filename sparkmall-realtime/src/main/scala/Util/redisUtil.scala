package Util

import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object redisUtil {
  var jedisPool:JedisPool=null

  def getJedisClient: Jedis = {
    if(jedisPool==null){
      println("开辟一个连接池")

      val host = SparkmallUtil.getValFromConfig("redis.host")
      val port = SparkmallUtil.getValFromConfig("redis.port")
      val auth = SparkmallUtil.getValFromConfig("redis.auth")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)  //最大连接数
      jedisPoolConfig.setMaxIdle(20)   //最大空闲
      jedisPoolConfig.setMinIdle(20)     //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(10000)//忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试


     jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt,10000,auth)
    }
   println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
   println("获得一个连接")
    jedisPool.getResource
  }



}
