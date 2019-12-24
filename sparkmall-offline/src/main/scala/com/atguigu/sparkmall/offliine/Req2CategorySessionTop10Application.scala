package com.atguigu.sparkmall.offliine

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.bean.DataModule.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
/*
Top10 热门品类中 Top10 活跃 Session 统计
 */
object Req2CategorySessionTop10Application {
  def main(args: Array[String]): Unit = {
    //TODO:1 获取hive表中的数据
    //创建配置信息
    val conf = new SparkConf().setAppName("Req1CategoryTop10Application").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //引入隐式转换
    import sparkSession.implicits._
    val sql = new StringBuilder("select * from user_visit_action where 1 = 1 ")
    //从hive中查询数据

    val startTime = SparkmallUtil.getValFromCondition("startDate")
    if (SparkmallUtil.isNotEmptyString(startTime)) {
      sql.append(" and action_time >= '").append(startTime).append("' ")
    }
    val endTime = SparkmallUtil.getValFromCondition("endDate")
    if (SparkmallUtil.isNotEmptyString(endTime)) {
      sql.append(" and action_time <= '").append(endTime).append("' ")
    }
    sparkSession.sql("use" + " " + SparkmallUtil.getValFromConfig("hive.database"));
    //需要将dataframe先赋予对象在转换为rdd,否则rdd返回值为row,不好处理
    val dataframe: DataFrame = sparkSession.sql(sql.toString())

    //将查询结果转换为RDD
    val userVisitActionRdd: RDD[UserVisitAction] = dataframe.as[UserVisitAction].rdd
    //println(userVisitActionRdd.count())

    //创建累加器
    val acc = new CategoryAccumulator
    //注册累加器
    sparkSession.sparkContext.register(acc)
    //TODO: 3 对原始数据进行便利，实现累加聚合操作
    //对数据进行累加处理
    userVisitActionRdd.foreach(action => {
      if (action.click_category_id != -1) {
        //点击
        acc.add(action.click_category_id + "_click")
      } else {
        if (action.order_category_ids != null) {
          val ids = action.order_category_ids.split(",")
          //下单
          for (id <- ids) {
            acc.add(id + "_order")
          }
        } else {
          if (action.pay_category_ids != null) {
            val ids = action.pay_category_ids.split(",")
            for (id <- ids) {
              acc.add(id + "_pay")
            }
          }
        }
      }
    })
    //获取累加器的执行结果
    val statResult: mutable.HashMap[String, Long] = acc.value

    //TODO:4将累加的结果进行分组合并
    //（categoryid, Map( categoryId-clickclickCount,  -orderorderCount, category-paypayCount ))
    val groupMap: Map[String, mutable.HashMap[String, Long]] = statResult.groupBy {
      case (k, v) => {
        k.split("_")(0)
      }
    }
    val taskId = UUID.randomUUID().toString()
    //TODO:5 将合并的数据转化为一条数据
    val list: List[CategoryTop10] = groupMap.map {
      case (categoryid, map) => {
        CategoryTop10(
          taskId,
          categoryid,
          map.getOrElse(categoryid + "_click", 0L),
          map.getOrElse(categoryid + "_order", 0L),
          map.getOrElse(categoryid + "_pay", 0L))

      }
    }.toList
    //TODO:6将数据排序后取前10条
    val top10: List[CategoryTop10] = list.sortWith {
      case (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }.take(10)
    //********************************需求2代码***************************************
   //TODO:1 获取热门品类的Top10的数据（需求1）
    val dataList = top10
    //TODO:2 将日志数据进行过滤，保留Top10品类的数据

    //拿到前10categoryid 的一个集合
    //List(4, 12, 17, 16, 5, 10, 18, 2, 13, 1)
     val category: List[String] = top10.map(_.categoryid)


    val filterRDD: RDD[UserVisitAction] = userVisitActionRdd .filter(action => {
      //判断click是否有值
      if (action.click_category_id != -1) {
        //error:类型不匹配，category:String,click_category_id:long
        category.contains(""+action.click_category_id)
      } else {
        false
      }
    })
    print(filterRDD.count())

    //TODO:3 将日志数据进行结构转换(  categoryId + sessionId, 1L )
    val categorySessionToCountRDD: RDD[(String, Long)] = filterRDD.map {
      case action => {
        (action.click_category_id + "_" + action.session_id, 1L)
      }
    }
    //TODO:4 将转换结构后的数据进行聚合：(  categoryId _ sessionId, 1L ) (  categoryId _ sessionId, sum)
    val categorySessionToSumRDD: RDD[(String, Long)] = categorySessionToCountRDD.reduceByKey(_+_)
    //TODO:5 将聚合的数据进行转换：(  categoryId _sessionId, sum)( categoryId, (sessionId, sum) )
     val categoryToSessionAndSum: RDD[(String, (String, Long))] = categorySessionToSumRDD.map {
       case (categorySession, sum) => {
         val ks: Array[String] = categorySession.split("_")
         (ks(0), (ks(1), sum))
       }
     }

    //TODO:6 将转换结构后的数据进行分组：( categoryId, (sessionId, sum) ) ( categoryId, List(sessionId, sum) )
     val group: RDD[(String, Iterable[(String, (String, Long))])] = categoryToSessionAndSum.groupBy {
       case (k, v) => {
         k
       }
     }

    //TODO:7 将分组后的数据进行排序（倒序），取前10条
     val sortRDD: RDD[(String, List[(String, (String, Long))])] = group.mapValues(datas => {
       datas.toList.sortWith {
         case (left, right) => {
           left._2._2 > right._2._2

         }
       }.take(10)
     })

    val mapList: RDD[List[(String, (String, Long))]] = sortRDD.map(_._2)
    //不能用_,因为推断不出来
    val flatMapRdd: RDD[(String, (String, Long))] = mapList.flatMap(x=>x)
    //println(flatMapRdd.count())
   //TODO:8 将结构通过JDBC保存到Mysql中
   val driverClass = SparkmallUtil.getValFromConfig("jdbc.driver.class")
    val url = SparkmallUtil.getValFromConfig("jdbc.url")
    val user = SparkmallUtil.getValFromConfig("jdbc.user")
    val password = SparkmallUtil.getValFromConfig("jdbc.password")
    Class.forName(driverClass)


//    flatMapRdd.foreach(datas=>{
//      statement.setObject(1,taskid)
//      statement.setObject(2,datas._1)
//      statement.setObject(3,datas._2._1)
//      statement.setObject(4,datas._2._2)
//
//      statement.executeUpdate()
//    })


    //将statement放入case中，防止序列化异常，但是foreach效率低，换成foreachPartition
   /* flatMapRdd.foreach {
      case (catergoryId,(sessionId,sum)) => {
        val connection: Connection = DriverManager.getConnection(url, user, password)
        val inserSql = "insert into category_top10_session_count values(?,?,?,?)"
        val statement: PreparedStatement = connection.prepareStatement(inserSql)
        statement.setObject(1,taskid)
        statement.setObject(2,catergoryId)
        statement.setObject(3,sessionId)
        statement.setObject(4,sum)
        statement.executeUpdate()
        statement.close()
        connection.close()
        sparkSession.close()

      }
    }
    */
  flatMapRdd.foreachPartition(datas=>{
    val connection: Connection = DriverManager.getConnection(url, user, password)
    val inserSql = "insert into category_top10_session_count values(?,?,?,?)"
    val statement: PreparedStatement = connection.prepareStatement(inserSql)
    for((catergoryId,(sessionId,sum)) <- datas){
      statement.setObject(1,taskId)
      statement.setObject(2,catergoryId)
      statement.setObject(3,sessionId)
      statement.setObject(4,sum)
      statement.executeUpdate()
    }
    statement.close()
    connection.close()

  })
    sparkSession.stop()

  }





    //************************************************************************

    //TODO:将结果保存到mysql数据库中
//    val driverClass = SparkmallUtil.getValFromConfig("jdbc.driver.class")
//    val url = SparkmallUtil.getValFromConfig("jdbc.url")
//    val user = SparkmallUtil.getValFromConfig("jdbc.user")
//    val password = SparkmallUtil.getValFromConfig("jdbc.password")
//    Class.forName(driverClass)
//    val connection: Connection = DriverManager.getConnection(url, user, password)
//    val inserSql = "insert into category_top10 values(?,?,?,?,?)"
//    val statement: PreparedStatement = connection.prepareStatement(inserSql)
//    top10.foreach(data=>{
//      statement.setObject(1,data.taskid)
//      statement.setObject(2,data.categoryid)
//      statement.setObject(3,data.clickCount)
//      statement.setObject(4,data.orderCount)
//      statement.setObject(5,data.payCount)
//      statement.executeUpdate()
//    })
//    statement.close()
//    connection.close()
//    sparkSession.close()
//
//  }
case class CategoryTop10(taskid:String,categoryid:String,clickCount:Long,orderCount:Long,payCount:Long)
  //TODO:2 累加器 AccumulatorV2[IN,OUT]
  //IN：drive 给 executor
  //out:executor将累加结果返回给driver
  //累加器里的结构（categroy-click,100)(category-order,50)(category-pay,10) map结构
  class CategoryAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{
    var map =new mutable.HashMap[String,Long]()

    //是否为初始值，先调copy-reset-zero
    override def isZero: Boolean = {
      map.isEmpty
    }
    //复制累加器,目的传播
    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
      new CategoryAccumulator
    }
   //重置累加器防止大量数据
    override def reset(): Unit = {
      map.clear()
    }
  //累加数据
    override def add(key: String): Unit = {
      //更新Map的值,若没有key,则为0；在executor中累加
      map(key) = map.getOrElse(key,0L)+1
    }

  //合并数据，exector中的完成数据和Driver中的数据合并，exector和driver中数据为map,用foldleft合并两个map
    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
      val map1 = map
      val map2 = other.value
      map = map1.foldLeft(map2){
        case (tempMap,(category,sumCount)) =>{
          tempMap(category)=tempMap.getOrElse(category,0L) + sumCount
          tempMap
        }
      }
    }

    /**
      * 累加器的值,直接返回给driver
      * @return
      */
    override def value: mutable.HashMap[String, Long] = map
  }

}
