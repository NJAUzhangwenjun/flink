package cn.wjhub.scala

/**
 * @Description:
 * @author 张文军
 * @Company:南京农业大学工学院
 * @version:1.0
 * @date 2020/12/2523:37
 */
object T1 extends App {
  private val p: P = new P()
  println(p)
  println(p.getClass.getClassLoader.getClass)
  println(classOf[P])
}
class P{

}
