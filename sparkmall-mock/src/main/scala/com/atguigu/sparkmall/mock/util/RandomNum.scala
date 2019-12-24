package com.atguigu.sparkmall.mock.util

import java.util.Random

object RandomNum {
  def apply(fromNum: Int, toNum: Int): Int = {
    fromNum + new Random().nextInt(toNum - fromNum + 1)
  }

  def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean) = {
    "1,2,3"
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个


  }
}
