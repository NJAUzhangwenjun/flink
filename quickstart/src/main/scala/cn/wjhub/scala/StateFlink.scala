package cn.wjhub.scala

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.util.Random


/**
 * @Description:
 * @author 张文军
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2021/1/109:18
 */
object StateFlink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
    val ds = env.socketTextStream("localhost", 9000)

    val data: DataStream[(String, Int, Long)] = ds.

      flatMap(
        _
          .replaceAll("\\W", " ")
          .split(" ")
          .filter(_.nonEmpty)
      )
      .map((_, 1, System.currentTimeMillis() + Random.nextInt()))
      .keyBy(0)
      .reduce((t1, t2) => {
        (t1._1, t1._2 + t2._2, t2._3)
      })


    val wordMap: DataStream[(String, Int)] = ds.flatMap(new FlatMapFunction[String, (String, Int)] {
      override def flatMap(value: String, out: Collector[(String, Int)]) = {
        val s: Array[String] = value.replaceAll("\\W", " ").split(" ")
        for (s1 <- s) {
          out.collect(s1, 1)
        }
      }
    })

    wordMap.keyBy(0).reduce((w1, w2) => (w1._1, w1._2 + w2._2)).print()


    data.print("data")

    env.execute("stateTest")


  }
}



