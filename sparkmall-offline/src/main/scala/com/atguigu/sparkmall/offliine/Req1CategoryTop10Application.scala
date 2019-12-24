package com.atguigu.sparkmall.offliine

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.bean.DataModule.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.SparkmallUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

object Req1CategoryTop10Application {
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
    val taskid = UUID.randomUUID().toString()
    //TODO:5 将合并的数据转化为一条数据
    val list: List[CategoryTop10] = groupMap.map {
      case (categoryid, map) => {
        CategoryTop10(
          taskid,
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

    //TODO:将结果保存到mysql数据库中
    val driverClass = SparkmallUtil.getValFromConfig("jdbc.driver.class")
    val url = SparkmallUtil.getValFromConfig("jdbc.url")
    val user = SparkmallUtil.getValFromConfig("jdbc.user")
    val password = SparkmallUtil.getValFromConfig("jdbc.password")
    Class.forName(driverClass)
    val connection: Connection = DriverManager.getConnection(url, user, password)
    val inserSql = "insert into category_top10 values(?,?,?,?,?)"
    val statement: PreparedStatement = connection.prepareStatement(inserSql)
    top10.foreach(data=>{
      statement.setObject(1,data.taskid)
      statement.setObject(2,data.categoryid)
      statement.setObject(3,data.clickCount)
      statement.setObject(4,data.orderCount)
      statement.setObject(5,data.payCount)
      statement.executeUpdate()
    })
    statement.close()
    connection.close()
    sparkSession.close()

  }
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
