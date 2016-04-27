import org.junit.Test

class Demo {
  val inc2 = (x: Int) => x + 1
  val a = (x: Int) => x + 1
  val fun = { i: Int => i * 2 }

  @Test
  def test21(): Unit = {
    val map = Map(1 -> "a", 2 -> "b")
    println(map.filter(t => t._2 == "a"))
    println(map.filter({ case (id, name) => name == "b" }))
    println(map.filter(t => t._2 == "b"))
  }


  @Test
  def test20(): Unit = {
    println(List(List(1, 2), List(3, 4, 5)).flatMap(list => list.map(_ * 1)))
    println(List(List(1, 2), List(3, 4, 5)).flatten)
    val list = 4 :: 3 :: 2 :: 1 :: Nil
    println(list)
  }


  @Test
  def test19(): Unit = {
    val result: List[(Int, String)] = List(1, 2, 3).zip(List("a", "b", "c"))
    println(result)
  }

  @Test
  def test18(): Unit = {
    val tuple = (1, "a")
    println(tuple match {
      case (id, name) => s"id=${id},name=${name}"
      case (1, name) => s"${name}"
    })
  }

  @Test
  def test17(): Unit = {
    try {
      throw new RuntimeException("haha")
    } catch {
      case e: Exception => println(e)
    }
  }

  @Test
  def test16(): Unit = {
    println(calcLevel(User(1, "a")))
    println(calcLevel(User(2, "b")))
    println(calcLevel(User(3, "c")))
    println(calcLevel("haha"))
    println(calcLevel(User(4, "d")))
  }

  def calcLevel(user: Any) = user match {
    case User(1, "a") => "A"
    case User(2, "b") => "B"
    //    case User(id, name) => s"${id},${name} unknown"
    case u@User(_, _) => s"${u}"
    case _ => "error"
  }

  @Test
  def test15(): Unit = {
    val user = User(1, "flyingh")
    val user2 = User(1, "flyingh")
    println(user == user2)
    println(user)
    println(user2)
  }

  @Test
  def test14(): Unit = {
    println(bigger(5))
    println(bigger("flyingh"))
  }

  def bigger(x: Any): Any = {
    x match {
      case i: Int if i < 0 => i - 1
      case i: Int => i + 1
      case d: Double if d < 0.0 => d - 0.1
      case d: Double => d + 0.1
      case text: String => "haha," + text
    }
  }

  @Test
  def test13(): Unit = {
    val a = 5
    val result = a match {
      case 3 => "a"
      case 5 => "b"
      case _ => "c"
    }
    println(result)
    val res2 = a match {
      case i if i == 5 => "5"
      case i if i != 5 => "!5"
    }
    println(res2)
  }

  @Test
  def test11(): Unit = {
    println(Info.a)
    println(Info.b)
  }

  @Test
  def test10(): Unit = {
    val a = new A()
  }

  @Test
  def test9(): Unit = {
    val c = new Calculator("a")
    println(c.color)
    println(c.add(1, 5))
  }

  @Test
  def test8(): Unit = {
    printAll("hello", "world")
  }

  def printAll(args: String*) = args.map(_.capitalize).foreach(println)

  @Test
  def test7(): Unit = {
    println(fun(2))
    println(fun)
  }

  @Test
  def test6(): Unit = {
    println(doubleInt(5))
  }

  def doubleInt(x: Int) = {
    println("Hello world!")
    x * 2
  }

  @Test
  def test5(): Unit = {
    println(a)
    println(a(3))
  }

  @Test
  def test4(): Unit = {
    println(inc)
    println(inc(3))
    println(inc2)
    println(inc2(3))
    println("############")
    println(inc3)
    println(inc3())
    println(inc3() == inc3)
    println(inc3() == inc3())
    println(inc3()(4))
  }

  def inc = (x: Int) => x + 1

  def inc3() = (x: Int) => x + 1

  @Test
  def test3(): Unit = {
    println(three)
    println(four())
    println(four)
  }

  def three = 1 + 2

  def four() = 2 + 2

  @Test
  def test2(): Unit = {
    val result = addOne(2)
    println(result)
  }

  @Test
  def test(): Unit = {
    println(addOne(1))
    println(addOne(2))
  }

  def addOne(m: Int): Int = m + 1

  @Test
  def test12(): Unit = {
    val a = new A()
    println(a(3))
  }

  case class User(id: Int, name: String)

  class A {
    def apply(m: Int) = m + 1
  }

  class Inc extends (Int => Int) {
    override def apply(v: Int): Int = v + 1
  }

  class Person {
    val id: Int = 0
    val name: String = "info"

    def add(m: Int, n: Int) = m + n
  }

  class Calculator(brand: String) {
    val color = if (brand == "a") {
      "A"
    } else if (brand == "b") {
      "B"
    } else if (brand == "c") {
      "C"
    }

    def add(m: Int, n: Int): Int = m + n
  }

  object Info {
    val a = "haha"
    val b = "hehe"
  }

}
