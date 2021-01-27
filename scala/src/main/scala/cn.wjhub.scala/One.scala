//package cn.wjhub.scala
//
///**
// * @Description:
// * @author 张文军
// * @Company:南京农业大学工学院
// * @version:1.0
// * @date 2020/12/2222:47
// */
//object One {
//  def main(args: Array[String]): Unit = {
//    val zhangsan: Pp = new Pp("zhangsan")
//    val zhangsan1: Pp = new Pp("zhangsan", "women")
//    zhangsan.eat()
//    zhangsan1.eat()
//    printf(zhangsan1.toString)
//    print("~~~~~~~~~~~~~~~~~~~~~~~~~")
//    zhangsan.apply("zhangsan")
//    println(zhangsan("zhang").toString)
//
//
//  }
//
//}
//
//private class Pp(name: String) {//主构造器
//  private var gender: String = _
//  print(name)
//
//  def eat(): Unit = {
//    println(name + " : eat....,gender is " + gender)
//  }
//
//  def this(name: String, gender: String) {/** 附属构造器*/
//    this(name)
//    this.gender = gender
//  }
//
//  override def toString: String = (name,gender).toString()
//
//  def apply(name: String): Pp = new Pp(name)
//}
//object Pp{
//  def apply(name: String): Pp = new Pp(name)
//}
