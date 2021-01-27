package cn.wjhub.scala

import scala.io.{BufferedSource, Source}

/**
 * @Description:
 * @author 张文军
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2020/12/232:56
 */
object WC extends App {

  private val list: List[(String, Int)] = Source.fromFile("E:/e/workeSpace/bigdata/flink/flinkWC.txt").getLines()
    .flatMap(_.split(" ").filter(_.nonEmpty).map((_, 1)))
    .toList
    .groupBy(_._1)
    .map(t => (t._1, t._2.size))
    .toList.sortWith(_._2 > _._2)

  list.foreach(println)

  println("---------------------")
  private val map: Map[String, Int] = Source.fromFile("E:/e/workeSpace/bigdata/flink/flinkWC.txt").getLines()
    .flatMap(_.split(" ").filter(_.nonEmpty).map((_, 1)))
    .toList
    .groupBy(_._1)
    .map(m => (m._1, m._2.size))

}
