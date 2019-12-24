package com.atguigu.bigdata.sparkmall.common.util

import java.io.InputStream
import java.text.SimpleDateFormat
import java.util.{Date, ResourceBundle}

import com.alibaba.fastjson.JSON
import org.apache.commons.net.ntp.TimeStamp

/*
电商项目工具类，读取配置文件
 */

   object SparkmallUtil {
     /*
          //TODO:fileName配置文件的名称,key文件当中的key
         def getVal(fileName:String,key:String) :String = {
           //通过类的加载器读取类加载器
           val filename: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(fileName)
           //读取后将配置文件转换为配置对象
           val properties = new java.util.Properties()
           //在通过对象的方法取到指定的属性
           properties.load(filename)
           properties.getProperty(key)

         }
    */

    //todo:使用国际化(i18n)组件读取配置文件
     //todo:只能读取properties文件
     //传参数是不需要添加扩展名
     def main(args: Array[String]): Unit = {
       //println(getValFromConfig("jdbc.url"))
       //println(getValFromCondition("endDate"))
     }

     def getValFromConfig(key:String)={
       getVal("config",key);
     }
    def getValFromCondition(key:String) : String={
      val condition: String = getVal("condition","condition.params.json")
      //将json字符串进行转换
      val jSONObject = JSON.parseObject(condition)
      jSONObject.getString(key)

    }
     //判断字符串是否为空，true 非空，false 为空
     def isNotEmptyString(s : String) : Boolean = {
      s != null && !" ".equals(s.trim)

     }
     //将时间字符串转换为日期对象
     def formatDateFromString(time:String,formatString: String="yyyy-MM-dd HH:mm:ss"):Date ={
       val format = new SimpleDateFormat(formatString)
       format.parse(time)

     }
     //将时间戳转换为日期字符串
     def parseStringDateFromTs(timeStamp:Long=System.currentTimeMillis(),formatString:String = "yyyy-MM-dd HH:mm:ss")={
       val format: SimpleDateFormat = new SimpleDateFormat(formatString)
       format.format(new Date(timeStamp))
     }


     def getVal(fileName:String,key:String):String={
       val bundle = ResourceBundle.getBundle(fileName)
       bundle.getString(key)

     }

}
