package com.flyingh.vo

import com.flyingh.demo.A

/**
  * Created by Flycoding on 2016/4/27.
  */
class Person(id: Int, name: String) {
  def bar(): Unit = {
    val a = new A
    a.foo()
  }
}
