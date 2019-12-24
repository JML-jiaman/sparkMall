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
页面单跳转化率统计
 */
object Req3PageFlowApplication {
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


    //**********************需求3*********************************
    // TODO:获取分母,每一个页面点击次数总和
    //1.将日志数据进行过滤,保留需要统计的页面
    //得到页面数据的集合,并切分
    val pageid: String = SparkmallUtil.getValFromCondition("targetPageFlow")
    val pageids: Array[String] = pageid.split(",")
    val filterRdd: RDD[UserVisitAction] = userVisitActionRdd.filter(action => {
      pageids.contains("" + action.page_id)//返回值为boolean值
    })


    //转换结构 (action==>pageId,1L)
    val pageidToCountRDD: RDD[(Long, Long)] = filterRdd.map(action => {
      (action.page_id, 1L)
    })

    //聚合(页面:1,点击次数30)
    val pageidToSumRDD: RDD[(Long, Long)] = pageidToCountRDD.reduceByKey(_+_)
    //pageidToSumRDD.foreach(println)
    //将数据进行转换，转换为map
    val tuples: Array[(Long, Long)] = pageidToSumRDD.collect()
    val map: Map[Long, Long] = tuples.toMap

    //TODO:获取分子
    //根据session进行分组,根据全部数据进行分组，保证数据的完整性
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRdd.groupBy(action => {
      action.session_id
    })

    //取v值，转换成list,按照时间排序

    //(String, List[(String, Long)])==>list((1-2),1)
    val sessionToPageCountList: RDD[(String, List[(String, Long)])] = groupRDD.mapValues(datas => {
      val sortList: List[UserVisitAction] = datas.toList.sortWith {
        case (left, right) => {
          left.action_time > right.action_time
        }
      }
      //List(1,2,5,4,6)
      val pageidList: List[Long] = sortList.map(_.page_id)
      //拉链zip
      //((1-2,1),(2-3,1),第一个集合与第一个集合去掉头部做拉链
      val pageFlowList: List[(Long, Long)] = pageidList.zip(pageidList.tail)
    pageFlowList.map {
        case (before, after) => {
          (before + "_" + after, 1L)
        }
      }

    })
    //取出分子的value
    val pageflowToCountListRdd: RDD[List[(String, Long)]] = sessionToPageCountList.map {
      case (session, list) => {
        list
      }

    }
    //将分子value扁平化
    val value: RDD[(String, Long)] = pageflowToCountListRdd.flatMap(x=>x)
    //根据分母拉链将不需要关心页面流转数据过滤掉
    //1-2，2-3，3-4
    val zipPageIds: Array[(String, String)] = pageids.zip(pageids.tail)
    val Strings = zipPageIds.map {
      case (before, after) => {
        before + "_" + after
      }
    }
    val filterZipRDD: RDD[(String, Long)] = value.filter {
      case (pageflow, count) => {
        Strings.contains(pageflow)
      }
    }

    val pageflowToSumRDD: RDD[(String, Long)] =  filterZipRDD.reduceByKey(_+_)


    //todo:页面单跳点击次数/页面点击次数
    pageflowToSumRDD.foreach {
      case (pageflow, sum) => {
        val ks: Array[String] = pageflow.split("_")
        println(pageflow + "转化率 = " + (sum.toDouble / map(ks(0).toLong)) * 100)
      }

    }










    //**********************************************************************
    //TODO:将结果保存到mysql数据库中
        val driverClass = SparkmallUtil.getValFromConfig("jdbc.driver.class")
        val url = SparkmallUtil.getValFromConfig("jdbc.url")
        val user = SparkmallUtil.getValFromConfig("jdbc.user")
        val password = SparkmallUtil.getValFromConfig("jdbc.password")
        Class.forName(driverClass)
        val connection: Connection = DriverManager.getConnection(url, user, password)
        val inserSql = "insert into 03category_jump values(?,?,?)"
        val statement: PreparedStatement = connection.prepareStatement(inserSql)



        statement.close()
        connection.close()
        sparkSession.close()

    //
    //  }
    //case class CategoryTop10(taskid:String,categoryid:String,clickCount:Long,orderCount:Long,payCount:Long)
    //  //TODO:2 累加器 AccumulatorV2[IN,OUT]
    //  //IN：drive 给 executor
    //  //out:executor将累加结果返回给driver
    //  //累加器里的结构（categroy-click,100)(category-order,50)(category-pay,10) map结构
    //  class CategoryAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{
    //    var map =new mutable.HashMap[String,Long]()
    //
    //    //是否为初始值，先调copy-reset-zero
    //    override def isZero: Boolean = {
    //      map.isEmpty
    //    }
    //    //复制累加器,目的传播
    //    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    //      new CategoryAccumulator
    //    }
    //   //重置累加器防止大量数据
    //    override def reset(): Unit = {
    //      map.clear()
    //    }
    //  //累加数据
    //    override def add(key: String): Unit = {
    //      //更新Map的值,若没有key,则为0；在executor中累加
    //      map(key) = map.getOrElse(key,0L)+1
    //    }
    //
    //  //合并数据，exector中的完成数据和Driver中的数据合并，exector和driver中数据为map,用foldleft合并两个map
    //    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    //      val map1 = map
    //      val map2 = other.value
    //      map = map1.foldLeft(map2){
    //        case (tempMap,(category,sumCount)) =>{
    //          tempMap(category)=tempMap.getOrElse(category,0L) + sumCount
    //          tempMap
    //        }
    //      }
    //    }
    //
    //    /**
    //      * 累加器的值,直接返回给driver
    //      * @return
    //      */
    //    override def value: mutable.HashMap[String, Long] = map
    //  }

  }
}