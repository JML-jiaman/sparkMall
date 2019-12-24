import Util.{kafkaUtil, redisUtil}
import com.atguigu.bigdata.sparkmall.common.bean.DataModule.KafkaMessages
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

// 广告黑名单实时统计
object Req5DateAreaCityAndCountApplication{

  /*def main(args: Array[String]): Unit = {
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
   //将采集周期数据进行聚合
    val mapDstream = messageDstream.map(message => {
      val dataString: String = SparkmallUtil.parseStringDateFromTs(message.timestamp.toLong)
      (dataString + "_" + message.area + "_" + message.city + "_" + message.adid, 1)
    })
    streamingContext.sparkContext.setCheckpointDir("cp")
    //采集的最后结果，存放在checkpoint
    val dataDstream = mapDstream.updateStateByKey {
      case (seq, opt) => {
        //将采集周期的统计结果和checkpoint中缓存的结果进行累加
        val sum = seq.sum + opt.getOrElse(0)
        //将累加结果放会checkpoint中
        Option(sum)
      }
    }
    //将聚合的数据跟新到redis中
     dataDstream.foreachRDD(rdd=>{
       rdd.foreachPartition(datas=>{
         val jedis: Jedis = redisUtil.getJedisClient
         for ((field,sum)<- datas) {
           val key ="date:area:city:ads"
            jedis.hset(key,field,""+sum)
         }
       })
     })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
  */
}
