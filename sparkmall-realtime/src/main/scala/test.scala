import com.atguigu.bigdata.sparkmall.common.bean.DataModule.KafkaMessages
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object test {
  def main(args: Array[String]): Unit = {
//    val jedis = new Jedis("hadoop102",6379)
//   jedis.auth("root")
//    println(jedis.ping())
//    jedis.close()
    //TODO:获取数据,将数据根据窗口滑动的幅度进行分组
    //将数据进行结构转换(kafkaMassage)==>(time,1)
    //(15:15:10,1),(15:15:12,1),((15:15:21,1)-->(15:15:10,1),(15:15:10,1),(15:15:20,1)


      var time: String = SparkmallUtil.parseStringDateFromTs(System.currentTimeMillis(),"mm:ss")
    val prefix: String = time.substring(0,4)
     time = prefix + "0"
    println(System.currentTimeMillis())
  println(time)

  }

}
