package cn.wjhub.scala

/**
 * @Description:
 * @author 张文军
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2020/12/232:26
 */
object HigherOrderFunction extends App {

   val l: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8)

  private val tuples: List[(Int, Int)] = l.map(x => (1, x * 10))

  println(tuples.mkString(","))

  private val i: Int = l.fold(0)(_ + _)
  print(i)


  l.reduce(_+_)

}
