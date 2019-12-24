import java.{lang, util}

import Util.{kafkaUtil, redisUtil}
import com.atguigu.bigdata.sparkmall.common.bean.DataModule.KafkaMessages
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

// 广告黑名单实时统计
object Req5RedisDateAreaCityAndCountApplication{

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Req4RedisUserBlackListAplication").setMaster("local[*]")
     val streamingContext = new StreamingContext(conf,Seconds(5))

    //从kafka中周期性获取广告点击数据
    val topic = "ads_log181111"
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = kafkaUtil.getKafkaStream(topic,streamingContext)
    //将数据进行分解和转换
    val messageDstream = kafkaDstream.map(record => {
      val messages: String = record.value()
      val datas: Array[String] = messages.split(" ")
      KafkaMessages(datas(0), datas(1), datas(2), datas(3), datas(4))

    })
    messageDstream.foreachRDD(rdd=>{

      val jedis = redisUtil.getJedisClient
      rdd.foreachPartition(messages=>{
        for (message <- messages) {
          val datas: String = SparkmallUtil.parseStringDateFromTs(message.timestamp.toLong,"yyyy-MM-dd")
          val key = "data:area:city:ads"
          val field: String = datas + "_"+ message.area + "_" + message.city + "_" + message.adid
          jedis.hincrBy(key,field,1)
        }
        jedis.close()
      })

  })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
