package cn.wjhub.scala

import javafx.beans.binding.SetBinding

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @Description:
 * @author 张文军
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2020/12/230:41
 */
object Collection extends App {
  private val l: List[Int] = List(1, 2, 3, 2, 3)
  private val l2: List[Int] = l.flatMap(i => (0.to(i)))
  println(l.mkString(","))
  println(l2.mkString(","))

}
