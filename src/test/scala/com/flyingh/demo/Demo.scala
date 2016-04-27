package com.flyingh.demo

import org.junit.Test

/**
  * Created by Flycoding on 2016/5/1.
  */
class Demo {
  @Test
  def test3(): Unit = {
    val seq = Seq(true, false)
    for (flag <- seq) {
      flag match {
        case true => println("true")
        case false => println("false")
      }
    }
  }

  @Test
  def test2(): Unit = {
    val value = 35.27
    println(f"$value%.5f")
    val name = "flyingh"
    println(raw"hello\n$name!")
    println(s"hello\n$name!")
  }

  @Test
  def test(): Unit = {
    val name = "icoding"
    println(s"Hello,$name")
    println(s"Hello,${name}")
    println(s"$$")
    println("$")
  }

}
