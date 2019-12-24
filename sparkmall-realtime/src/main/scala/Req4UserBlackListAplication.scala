
import java.util

import Util.{kafkaUtil, redisUtil}
import com.atguigu.bigdata.sparkmall.common.bean.DataModule.KafkaMessages
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


object Req4UserBlackListAplication {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Req4UserBlackListAplication")
    val streamingContext: StreamingContext = new StreamingContext(conf,Seconds(5))
//   TODO:4.1 从kafka中获取用户点击广告的数据
    val topic = "ads_log181111"
    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = kafkaUtil.getKafkaStream(topic,streamingContext)
    //   TODO:4.2 将数据进行分解和转化：zs 2019-04-30 12:21:12 xxxxe(广告) （2019-04-30_zs_xxx,1）
    val messageDstream: DStream[KafkaMessages] = kafkaDstream.map { record => {
      val messages: String = record.value()
      val datas: Array[String] = messages.split(" ")
      KafkaMessages(datas(0), datas(1), datas(2), datas(3), datas(4))
    }

    }
    messageDstream.print()
    //将redis中黑名单过滤掉
    //从redis中获取黑名单
    /*//driver,只在driver初始时执行一次，数据周期性采取，所以有问题
    val client: Jedis = redisUtil.getJedisClient
    val blacklist: util.Set[String] = client.smembers("blacklist")

    val filerDstream: DStream[KafkaMessages] = messageDstream.filter(message => {

      !blacklist.contains(message.userid)
    })
    */

   val value = messageDstream.transform(rdd => {
      //只创建 一个连接
      val client: Jedis = redisUtil.getJedisClient
      //这个关键字是不能被序列化
      val blacklistSet: util.Set[String] = client.smembers("blacklist")
      //用广播变量
      val broadcastSet: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(blacklistSet)
      client.close()
      rdd.filter(message => {
        !broadcastSet.value.contains("blacklist")
      })

    })

    //   TODO:4.3 将转换的结果进行聚合(采集周期内)（2019-04-30_zs_xxx,sum）
    val mapStream: DStream[(String, Long)] = messageDstream.map(message => {
      val dataString: String = SparkmallUtil.parseStringDateFromTs(message.timestamp.toLong, formatString = "yyyy-MM-dd HH:mm:ss")
      (dataString + " " + message.userid + " " + message.adid, 1)
    })
    //TODO:设定checkpoint路径,在checkpoints中做累加
    streamingContext.sparkContext.setCheckpointDir("cp")
    //   TODO:4.4将不同周期中的采集数据进行累加（有状态）
        val stateDStream: DStream[(String, Long)] = mapStream.updateStateByKey {
          case (seq, opt) => {
            //将当前采集周期的统计结果和checkpoint中的数据进行累加
            val sum = seq.sum + opt.getOrElse(0L)
            //将累加结果放回到checkpoint中
            Option(sum)
          }
        }

//   TODO:4.5 累加 后的点击次数进行阈值（100）判断
    stateDStream.foreachRDD(rdd=>{
      rdd.foreach{
        case (key,sum) =>{
          if(sum >100){
            //   TODO:4.6 如果点击次数超过阈值，那么会将用户加入黑名单，防止用户继续访问
            //将数据更新到redis中
            val jedis: Jedis = redisUtil.getJedisClient
            //获取用户
            val useid: String = key.split("_")(1)
            //redis中加入黑名单
              jedis.sadd("blacklist",useid)
          }
        }
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
