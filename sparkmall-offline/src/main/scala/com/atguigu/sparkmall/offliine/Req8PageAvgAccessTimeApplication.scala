package com.atguigu.sparkmall.offliine

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.bigdata.sparkmall.common.bean.DataModule.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
页面单跳转化率统计
 */
object Req8PageAvgAccessTimeApplication {
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


    //**********************需求8*********************************
    //获取离线数据
    //将数据根据session进行分组
    val sessionGroupBy: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRdd.groupBy(action => {
      action.session_id
    })

    //将分组后的数据按照方向时间进行排序
    val sessionToPageIdTimeRdd: RDD[(String, List[(Long, Long)])] = sessionGroupBy.mapValues(datas => {
      val actions = datas.toList.sortWith {
        case (left, right) => {
          left.action_time < right.action_time
        }
      }

      //(A-time),(B-time),(C-time)
      val pageidToTimeList: List[(Long, String)] = actions.map(action => {
        (action.page_id, action.action_time)
      })
      //zip
      //(B-time)(c-time),(D-time)
      val zipList: List[((Long, String), (Long, String))] = pageidToTimeList.zip(pageidToTimeList.tail)
      //map==>(A,min1-min2)==>(A,time)
      zipList.map {
        case (before, after) => {
          val startTime = SparkmallUtil.formatDateFromString(before._2).getTime
          val endTime = SparkmallUtil.formatDateFromString(after._2).getTime
          (before._1, (endTime - startTime))
        }
      }

    })

    //采用拉链的方式将数据进行组合，形成页面跳转路径（A->B,B->C)
    val listRdd: RDD[List[(Long, Long)]] = sessionToPageIdTimeRdd.map {
      case (session, list) => {
        list
      }
    }
    //将集合数据进行扁平化操作
    val pageIdToTimeRDD: RDD[(Long, Long)] = listRdd.flatMap(x=>x)
    val pageIdToTimeListRDD: RDD[(Long, Iterable[Long])] = pageIdToTimeRDD.groupByKey()
    //对跳转路径中的第一个页面进行分组，聚合数据
    val pageIdToAvgTimeRdd: RDD[(Long, Long)] = pageIdToTimeListRDD.mapValues(datas => {
      //对聚合后的数据进行计算，获取结果
      datas.sum / datas.size
    })

    println(pageIdToAvgTimeRdd)










    //**********************************************************************

  }
}