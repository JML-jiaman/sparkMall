package com.atguigu.sparkmall.realtime

import java.util

import Util.{kafkaUtil, redisUtil}
import com.atguigu.bigdata.sparkmall.common.bean.DataModule.KafkaMessages
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

// 广告黑名单实时统计
object Req4RedisUserBlackListAplication {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Req4RedisUserBlackListAplication").setMaster("local[*]")
     val streamingContext = new StreamingContext(conf,Seconds(5))

    //从kafka中周期性获取广告点击数据
    val topic = "ads_log181111"
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = kafkaUtil.getKafkaStream(topic,streamingContext)
    //将数据进行分解和转换
    val messageDstream = kafkaDstream.map(record => {
      val message: String = record.value()
      val datas: Array[String] = message.split(" ")
      KafkaMessages(datas(0), datas(1), datas(2), datas(3), datas(4))

    })

    //将获取的数据进行筛选（过滤掉黑名单的数据）
    val filterDStream: DStream[KafkaMessages] = messageDstream .transform(rdd => {
      // Driver....
      val jedis: Jedis =redisUtil.getJedisClient
      val blackListSet:java.util.Set[String] = jedis.smembers("blacklist")
      // 广播变量
     val broadcastSet: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(blackListSet)

      jedis.close()
      rdd.filter(message => {
        // Executor
        //!blackListSet.contains(message.userid)
        !broadcastSet.value.contains(message.userid)
      })
    })

    filterDStream.foreachRDD(rdd=>{

      rdd.foreachPartition(messages=>{
        val jedis: Jedis = redisUtil.getJedisClient

        for (message <- messages) {
          val dateString: String = SparkmallUtil.parseStringDateFromTs(message.timestamp.toLong, "yyyy-MM-dd")
          //(dateString + "_" + message.userid + "_" + message.adid, 1)
          // 将redis中指定的key，进行数据累加
          // key => dateString + "_" + message.userid + "_" + message.adid
          val key = "date:user:ad:clickcount"
          val field = dateString + "_" + message.userid + "_" + message.adid

          jedis.hincrBy(key, field, 1)

          val sumString: String = jedis.hget(key, field)
          val sum = sumString.toLong

          if ( sum >= 100 ) {
            jedis.sadd("blacklist", message.userid)
          }
        }

        jedis.close()
      })


    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
