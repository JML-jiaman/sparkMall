import Util.{kafkaUtil, redisUtil}
import com.atguigu.bigdata.sparkmall.common.bean.DataModule.KafkaMessages
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

// 每天各地区 top3 热门广告




object Req6RedisDateAreaTop3Application{

  def main(args: Array[String]): Unit = {


//
//    val conf: SparkConf = new SparkConf().setAppName("Req4RedisUserBlackListAplication").setMaster("local[*]")
//
//
//     val streamingContext = new StreamingContext(conf,Seconds(5))
//
//    //从kafka中周期性获取广告点击数据
//    val topic = "ads_log181111"
//    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = kafkaUtil.getKafkaStream(topic,streamingContext)
//    //将数据进行分解和转换
//    val messageDstream = kafkaDstream.map(record => {
//      val messages: String = record.value()
//      val datas: Array[String] = messages.split(" ")
//      KafkaMessages(datas(0), datas(1), datas(2), datas(3), datas(4))
//
//    })
//    //将采集周期数据进行聚合
//    val mapDstream = messageDstream.map(message => {
//      val dataString: String = SparkmallUtil.parseStringDateFromTs(message.timestamp.toLong)
//      (dataString + "_" + message.area + "_" + message.city + "_" + message.adid, 1)
//    })
//    streamingContext.sparkContext.setCheckpointDir("cp")
//    //采集的最后结果，存放在checkpoint
//    val dataDstream = mapDstream.updateStateByKey {
//      case (seq, opt) => {
//        //将采集周期的统计结果和checkpoint中缓存的结果进行累加
//        val sum = seq.sum + opt.getOrElse(0)
//        //将累加结果放会checkpoint中
//        Option(sum)
//      }
//    }
//
//    //*********需求6***********
//    // TODO: 4.1 获取需求5的数据：（ date:area:city:advert, sum ）
//   //TODO: 4.2 将数据进行转换：（ date:area:city:advert, sum ）（ date:area:advert, sum1 ）, （ date:area:advert, sum2 ）, （ date:area:advert, sum3 ）
//    val dateAreaAdvToTotalSumDstream = dataDstream.map {
//
//      case (key, sum) => {
//        val keys = key.split("_")
//        val newKey = keys(0) + "_" + keys(1) + "_" + keys(3)
//        (newKey, sum)
//      }
//    }
//
//    //TODO: 4.3 将转换后的数据进行聚合：（ date:area:advert, sumTotal ）
//     val dataAreaAdvToTotalSumDstream: DStream[(String, Int)] =
//       dateAreaAdvToTotalSumDstream.reduceByKey(_+_)
//    //TODO: 4.4 将聚合后的数据进行结构转换：（ date:area:advert, sumTotal ）
//     // TODO:（ (date,area）(advert, sumTotal )）
//    val  dateAreaToAdvTotalSumDstream: DStream[(String, (String, Int))] = dataAreaAdvToTotalSumDstream.map {
//      case (key, sumTotal) => {
//        val mapKey: Array[String] = key.split("_")
//        ((mapKey(0) + "_" + mapKey(1)), (mapKey(2), sumTotal))
//
//      }
//
//    }
//
//
//    //TODO: 4.5 将转换结构后的数据进行分组排序（ (date,area）,Map(adv1totalSum1,adv2totalSum2)
//    val groupDstream: DStream[(String, Iterable[(String, Int)])] = dateAreaToAdvTotalSumDstream.groupByKey()
//    val sortDstream: DStream[(String, Map[String, Int])] = groupDstream.mapValues(datas => {
//      datas.toList.sortWith {
//        case (left, right) => {
//          left._2 > right._2
//        }
//      }.take(3).toMap
//
//    })
//
//    //TODO: 4.6获取前三的数据,将结果保存到Redis中（json的转换）
//   sortDstream.foreachRDD(rdd=>{
//     rdd.foreachPartition(datas=>{
//       val jedis: Jedis = redisUtil.getJedisClient
//
//       for ((k,map) <- datas) {
//        val ks = k.split("_")
//         val key ="top3_ads_per_day:"+ks(0)
//         //将scala集合转换为json字符串
//         import org.json4s.JsonDSL._
//         val value: String = JsonMethods.compact(JsonMethods.render(map))
//
//         jedis.hset(key,ks(1),value)
//       }
//
//       jedis.close()
//
//     })
//   })
//
//    streamingContext.start()
//    streamingContext.awaitTermination()
//
}

}