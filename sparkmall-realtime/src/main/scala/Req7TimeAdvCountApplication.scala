import Util.{kafkaUtil, redisUtil}
import com.atguigu.bigdata.sparkmall.common.bean.DataModule.KafkaMessages
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

// 最近一小时广告点击趋势


object Req7TimeAdvCountApplication{

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Req7RedisUserBlackListAplication").setMaster("local[*]")
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
    /*val mapDstream = messageDstream.map(message => {
      val dataString: String = SparkmallUtil.parseStringDateFromTs(message.timestamp.toLong)
      (dataString + "_" + message.area + "_" + message.city + "_" + message.adid, 1)
    })
    */


    //*********需求7*************
    //TODO:使用窗口函数将多个采集周期的数据作为一个整体进行统计
    val windowDstream: DStream[KafkaMessages] = messageDstream.window(Seconds(60),Seconds(10))
    //TODO:获取数据,将数据根据窗口滑动的幅度进行分组
    //将数据进行结构转换(kafkaMassage)==>(time,1)
    //(15:15:10,1),(15:15:12,1),((15:15:21,1)-->(15:15:10,1),(15:15:10,1),(15:15:20,1)
    val timeToClickDstream: DStream[(String, Int)] = windowDstream.map(message => {
      var time: String = SparkmallUtil.parseStringDateFromTs(System.currentTimeMillis(), "mm:ss")
      //截取前三个字母
      val prefix: String = time.substring(0, time.length-1)
      //与time拼接
      time = prefix + 0
      (time, 1)

    })
    //TODO:将分组后的数据进行统计聚合
    val reduceDstream: DStream[(String, Int)] = timeToClickDstream.reduceByKey(_+_)
    //TODO:将统计的结果按照时间进行排序
    //sortBy根据时间排序，t==>(String, Int)(时间，点击次数)
    val sortedDstream: DStream[(String, Int)] = reduceDstream.transform(rdd => {

      rdd.sortBy(t => {
        t._1
      }, false)
    })
    //TODO:将排序后的数据保存到Redis中
    sortedDstream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas=>{
        val jedis: Jedis = redisUtil.getJedisClient


        jedis.close()
      })
    })




   //***************************
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
