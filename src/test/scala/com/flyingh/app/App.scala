package com.flyingh.app

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Flycoding on 2016/4/27.
  */
class App {
  val sc = new SparkContext(new SparkConf().setAppName("app").setMaster("local[8]"))

  @Test
  def test87(): Unit = {
    sc.parallelize(1 to 10,5).zipWithUniqueId().takeOrdered(10).foreach(println)
  }

  @Test
  def test86(): Unit = {
    sc.parallelize(1 to 10,2).zipWithIndex().takeOrdered(10).foreach(println)
  }

  @Test
  def test85(): Unit = {
    sc.parallelize(1 to 10, 5).zipPartitions(sc.parallelize(List("a", "b", "c", "d", "e"), 5))((iter1, iter2) => {
      val list = ArrayBuffer[String]()
      while (iter1.hasNext && iter2.hasNext) {
        list += (iter2.next() + iter1.next())
      }
      list.iterator
    }).foreach(println)

  }

  @Test
  def test84(): Unit = {
    sc.parallelize(1 to 3).zip(sc.parallelize(List("a", "b", "c"))).foreach(println)
  }

  @Test
  def test83(): Unit = {
    val rdd: RDD[Int] = sc.parallelize(1 to 10, 2)
    println(rdd.variance())
    println(rdd.sampleVariance())
  }

  @Test
  def test82(): Unit = {
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3)))
    println(rdd.keys.collect.toList)
    println(rdd.values.collect().toList)
  }

  @Test
  def test81(): Unit = {
    val rdd1: RDD[Int] = sc.parallelize(1 to 10)
    val rdd2: RDD[Int] = rdd1 ++ rdd1
    rdd2.collect
    rdd2.unpersist()
  }

  @Test
  def test80(): Unit = {
    val rdd1: RDD[Int] = sc.parallelize(1 to 3)
    val rdd2: RDD[Int] = sc.parallelize(2 to 5)
    println(rdd1.union(rdd2).collect().toList)
    println((rdd1 ++ rdd2).collect().toList)
  }

  @Test
  def test79(): Unit = {
    println(sc.parallelize(1 to 100, 2).treeReduce(_ + _))
  }

  @Test
  def test78(): Unit = {
    val rdd: RDD[Int] = sc.parallelize(1 to 6, 2)
    rdd.mapPartitionsWithIndex((index, iter) => iter.map(item => (index, item))).groupByKey().foreach(println)
    println(rdd.treeAggregate(0)(math.max(_, _), _ + _))
    println(rdd.treeAggregate(5)(math.max(_, _), _ + _))
  }

  @Test
  def test77(): Unit = {
    println(sc.parallelize(1 to 10, 2).toString())
    println(sc.parallelize(List(("a", 1), ("c", 2))).sortByKey().toString())
    println(sc.parallelize(List(("a", 1), ("c", 2))).sortByKey().toDebugString)
  }

  @Test
  def test76(): Unit = {
    val rdd: RDD[Int] = sc.parallelize(1 to 10)
    println(rdd.takeOrdered(2).toList)
    println(rdd.top(2).toList)
  }

  @Test
  def test75(): Unit = {
    sc.parallelize(1 to 100, 50).toLocalIterator.foreach(println)
  }

  @Test
  def test74(): Unit = {
    println(sc.parallelize(1 to 10, 3).toJavaRDD())
  }

  @Test
  def test73(): Unit = {
    println(sc.parallelize(Nil, 3).toDebugString)
  }

  @Test
  def test72(): Unit = {
    val rdd: RDD[Int] = sc.parallelize(1 to 10, 2)
    println(rdd.takeSample(false, 100).toList)
    println(rdd.takeSample(true, 100).toList)
  }

  @Test
  def test71(): Unit = {
    println(sc.parallelize(List(7, 8, 6, 5, 9, 4, 1, 3, 2), 5).take(3).toList)
    println(sc.parallelize(List(7, 8, 6, 5, 9, 4, 1, 3, 2), 5).takeOrdered(3).toList)
  }

  @Test
  def test70(): Unit = {
    sc.parallelize(1 to 10, 5).take(3).foreach(println)
    println(sc.parallelize(1 to 10000, 5).take(100).toList)
  }

  @Test
  def test69(): Unit = {
    println(sc.parallelize(1 to 10).sum)
    println(sc.parallelize(1 to 10).sumApprox(100))
    println(sc.parallelize(1 to 10).sumApprox(100, 0.01))
  }

  @Test
  def test68(): Unit = {
    sc.parallelize(List(("a", 1), ("bc", 2), ("cde", 3))).keyBy(_._1.length)
      .subtractByKey(sc.parallelize(List(("de", 1), ("efg", 2))).keyBy(_._1.length))
      .foreach(println)
  }

  @Test
  def test67(): Unit = {
    sc.parallelize(1 to 10).subtract(sc.parallelize(1 to 10).filter(_ % 2 == 0)).foreach(println)
  }

  @Test
  def test66(): Unit = {
    val rdd: RDD[Int] = sc.parallelize(1 to 10)
    println(rdd.stdev())
    println(rdd.sampleStdev())
  }

  @Test
  def test65(): Unit = {
    sc.parallelize(List(("b", 2), ("c", 3), ("a", 1))).sortByKey(false, 1).foreach(println)
  }

  @Test
  def test64(): Unit = {
    sc.parallelize(List(3, 1, 5, 2, 4)).sortBy(t => t, true, 1).foreach(println)
    sc.parallelize(Array(("c", 3), ("a", 1), ("b", 2))).coalesce(1).sortBy(_._1).foreach(println)
  }

  @Test
  def test63(): Unit = {
    println(sc.parallelize(1 to 10, 2).stats())
  }

  @Test
  def test62(): Unit = {
    val path: String = "hdfs://flyme:9000/a/b/c"
    sc.parallelize(1 to 10, 2).saveAsTextFile(path)
    sc.textFile(path).foreach(println)
  }

  @Test
  def test61(): Unit = {
    sc.parallelize(1 to 10, 2).saveAsTextFile("textFiles")
    sc.parallelize(1 to 100, 2).saveAsTextFile("textFiles2", classOf[GzipCodec])
    println(sc.textFile("textFiles2").count())
  }

  @Test
  def test60(): Unit = {
    sc.parallelize(List(("a", 1), ("b", 2), ("c", 3)), 2).saveAsSequenceFile("seq_files")
  }

  @Test
  def test59(): Unit = {
    sc.parallelize(1 to 100, 5).saveAsObjectFile("objDir")
    val rdd: RDD[Int] = sc.objectFile[Int]("objDir")
    rdd.foreach(println)
  }

  @Test
  def test58(): Unit = {
    val rdd: RDD[(Int, String)] = sc.parallelize(List((7, "cat"), (6, "mouse"), (7, "cup"), (6, "book"), (7, "tv"), (6, "screen"), (7, "heater")))
    rdd.sampleByKey(false, Map((7, 0.4), (6, 0.6)), 42).foreach(println)
    println("*****************************")
    rdd.sampleByKeyExact(false, Map((7, 0.4), (6, 0.6)), 42).foreach(println)
  }

  @Test
  def test57(): Unit = {
    sc.parallelize(1 to 10, 5).sample(false, 0.35).foreach(println)
  }

  @Test
  def test56(): Unit = {
    sc.parallelize(List(("a", 1), ("b", 2), ("c", 3))).rightOuterJoin(sc.parallelize(List(("a", 5), ("e", 6), ("c", 4))))
      .foreach(println)
  }

  @Test
  def test55(): Unit = {
    val rdd: RDD[(Int, String)] = sc.parallelize(List((2, "cat"), (6, "mouse"), (7, "cup"), (3, "book"), (4, "tv"), (1, "screen"), (5, "heater")), 3)
    rdd.partitionBy(new RangePartitioner(3, rdd))
      .mapPartitionsWithIndex((index, iter) => iter.map(item => (index, item)))
      .foreach(println)
    println("********************")
    rdd.repartitionAndSortWithinPartitions(new RangePartitioner(3, rdd))
      .mapPartitionsWithIndex((index, iter) => iter.map(item => (index, item)))
      .foreach(println)

  }

  @Test
  def test54(): Unit = {
    val rdd: RDD[Int] = sc.parallelize(1 to 10000, 3)
    println(rdd.partitions.length)
    println(rdd.repartition(5).partitions.length)
    println(rdd.coalesce(8).partitions.length) //3
    println(rdd.coalesce(1).partitions.length)
    println(rdd.coalesce(2).partitions.length)
    println(rdd.coalesce(10).partitions.length) //3
    println(rdd.repartition(8).partitions.length)
  }

  @Test
  def test53(): Unit = {
    sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 3).keyBy(_.length).reduceByKey(_ + _).foreach(println)
  }

  @Test
  def test52(): Unit = {
    println(sc.parallelize(1 to 100, 3).reduce(_ + _))
  }

  @Test
  def test51(): Unit = {
    sc.parallelize(1 to 10, 3).randomSplit(Array(0.4, 0.6)).map(_.collect.toList).foreach(println)
    println("#######################")
    sc.parallelize(1 to 10, 3).randomSplit(Array(0.1, 0.3, 0.6)).map(_.collect.toList).foreach(println)
  }

  @Test
  def test50(): Unit = {
    sc.parallelize(1 to 10, 3).pipe("head -n -1").foreach(println)
  }

  @Test
  def test49(): Unit = {
    val rdd: RDD[Nothing] = sc.parallelize(Nil)
    println(rdd.getStorageLevel)
    rdd.cache()
    println(rdd.getStorageLevel)
  }

  @Test
  def test48(): Unit = {
    val rdd: RDD[Int] = sc.parallelize(Nil)
    println(rdd.partitioner)
    rdd.partitions.foreach(println)
    println(rdd.partitions.length)
  }

  @Test
  def test47(): Unit = {
    val rdd: RDD[Nothing] = sc.parallelize(Nil)
    println(rdd.name)
    rdd.setName("haha")
    println(rdd.name)
  }

  @Test
  def test46(): Unit = {
    println(sc.parallelize(1 to 10, 3).mean())
    println(sc.parallelize(1 to 10, 3).meanApprox(100, 0.001))
  }

  @Test
  def test45(): Unit = {
    val rdd: RDD[Int] = sc.parallelize(1 to 10, 3)
    println(rdd.max())
    println(rdd.min())
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)))
    println(rdd2.max())
    println(rdd2.min())
  }

  @Test
  def test44(): Unit = {
    sc.parallelize(List(("a", 1), ("b", 2), ("a", 3), ("c", 4), ("b", 5))).mapValues(_ => 1L).reduceByKey(_ + _).foreach(println)
  }

  @Test
  def test43(): Unit = {
    sc.parallelize(1 to 10, 3).mapPartitionsWithIndex((index, iter) => iter.map(item => (index, item))).groupByKey.foreach(println)
  }

  @Test
  def test42(): Unit = {
    sc.parallelize(1 to 10, 3).mapPartitions(iter => iter.flatMap(List.fill(3)(_))).countByValue.foreach(println)
    println("##################")
    sc.parallelize(1 to 10, 3).flatMap(List.fill(3)(_)).countByValue.foreach(println)
  }

  @Test
  def test41(): Unit = {
    sc.parallelize(List("a", "b", "c")).map(_.toUpperCase).foreach(println)
  }

  @Test
  def test40(): Unit = {
    sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("b", 4), ("c", 5), ("b", 0), ("d", 10))).lookup("b").foreach(println)
  }

  @Test
  def test39(): Unit = {
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("a", 3), ("d", 4), ("c", 5)))
    rdd1.leftOuterJoin(rdd2).map {
      case (key, (v1, v2)) => (key, v1, v2.getOrElse(-1))
    }.map {
      case (k, v1, v2) => s"$k,$v1,$v2"
    }.foreach(println)
  }

  @Test
  def test38(): Unit = {
    val rdd: RDD[String] = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 3)
    rdd.mapPartitionsWithIndex((index, iter) => {
      for (e <- iter) yield {
        (index, e)
      }
      iter.toList.map((index, _)).iterator
    }).foreach(println)
    rdd.keyBy(_.length).keys.foreach(println)
  }

  @Test
  def test37(): Unit = {
    sc.parallelize(List((1, "a"), (2, "b"), (3, "c"))).keys.foreach(println)
  }

  @Test
  def test36(): Unit = {
    sc.parallelize(1 to 10).keyBy(_ % 5).foreach {
      case (mod, v) => println(s"$mod,$v")
    }
  }

  @Test
  def test35(): Unit = {
    val rdd1: RDD[(Int, String)] = sc.parallelize(List((1, "a"), (2, "b"), (3, "c")))
    val rdd2: RDD[(Int, String)] = sc.parallelize(List((1, "first"), (2, "second"), (4, "fourth")))
    rdd1.join(rdd2).foreach {
      case (k, (v1, v2)) => println(s"${k}->(${v1},$v2)")
    }
  }

  @Test
  def test34(): Unit = {
    sc.setCheckpointDir("/a1/b1/c1")
    val rdd: RDD[Int] = sc.parallelize(1 to 10)
    println(rdd.isCheckpointed)
    rdd.checkpoint()
    println(rdd.isCheckpointed)
    rdd.collect
    println(rdd.isCheckpointed)
    println(rdd.getCheckpointFile)
  }

  @Test
  def test33(): Unit = {
    sc.parallelize(1 to 20).intersection(sc.parallelize(10 to 30)).collect().sorted.foreach(println)
  }

  @Test
  def test32(): Unit = {
    println(sc.parallelize(Nil).id)
    println(sc.parallelize(1 to 10).id)
  }

  @Test
  def test31(): Unit = {
    sc.parallelize(1 to 10).histogram(3) match {
      case (a1, a2) => println((a1.toList, a2.toList))
    }
    sc.parallelize(1 to 10).histogram(4) match {
      case (a1, a2) => println((a1.toList, a2.toList))
    }
  }

  @Test
  def test30(): Unit = {
    sc.parallelize(List("a", "bc", "de", "abc", "e")).keyBy(_.length).groupByKey().foreach(println)
  }

  @Test
  def test29(): Unit = {
    sc.parallelize(1 to 10).groupBy(_ % 2 == 0).map { case (flag, iter) => (flag, iter.toList) }.foreach(println)
  }

  @Test
  def test28(): Unit = {
    sc.parallelize(1 to 10, 3).glom().map(_.toList).foreach(println)
  }

  @Test
  def test27(): Unit = {
    val rdd: RDD[Nothing] = sc.parallelize(Nil)
    println(rdd.getStorageLevel)
    rdd.persist(StorageLevel.DISK_ONLY)
    println(rdd.getStorageLevel)
    rdd.cache()
  }

  @Test
  def test26(): Unit = {
    sc.setCheckpointDir("/a/b/c")
    val rdd: RDD[Int] = sc.parallelize(1 to 1000, 5)
    val rdd2 = rdd ++ rdd ++ rdd ++ rdd ++ rdd
    println(rdd2.getCheckpointFile)
    rdd2.checkpoint()
    println(rdd2.getCheckpointFile)
    rdd2.collect()
    println(rdd2.getCheckpointFile)
  }

  @Test
  def test25(): Unit = {
    val rdd1: RDD[(Int, String)] = sc.parallelize(List((1, "a"), (2, "b"), (3, "c")))
    val rdd2: RDD[(Int, String)] = sc.parallelize(List((1, "a"), (4, "d"), (3, "e")))
    rdd1.fullOuterJoin(rdd2).foreach(println)
  }

  @Test
  def test24(): Unit = {
    sc.parallelize(1 to 10).foreach(println)
    println("###################")
    sc.parallelize(1 until 10, 3).foreachPartition(iter => println(iter.reduce(_ + _)))
  }

  @Test
  def test23(): Unit = {
    val rdd: RDD[(Int, String)] = sc.parallelize(List("dog", "tiger", "cat", "lion", "eagle")).map(item => (item.length, item))
    rdd.foldByKey("")(_ + _).foreach(println)
    rdd.aggregateByKey("")(_ + _, _ + _).foreach(println)
  }

  @Test
  def test22(): Unit = {
    println(sc.parallelize(1 to 10).fold(0)(_ + _))
  }

  @Test
  def test21(): Unit = {
    val rdd = sc.parallelize(List("dog", "tiger", "cat", "lion", "eagle"), 1)
    rdd.map(item => (item.length, item)).flatMapValues("x" + _ + "x").foreach(println)
  }

  @Test
  def test20(): Unit = {
    List.fill(3)(5) foreach println
    println("################")
    for (c <- "hello world") {
      println(c)
    }
  }

  @Test
  def test19(): Unit = {
    sc.parallelize(List((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"))).sortByKey().filterByRange(2, 4).foreach(println)
  }

  @Test
  def test18(): Unit = {
    sc.parallelize(1 to 10).filter(_ % 2 == 0).foreach(println)
  }

  @Test
  def test17(): Unit = {
    println(sc.parallelize(List(1, 2, 3, 4, 5), 2).first)
  }

  @Test
  def test16(): Unit = {
    sc.parallelize(List(1, 2, 3, 1, 2, 3, 3, 2)).distinct.foreach(println)
    sc.parallelize(List("a", "b", "c", "b", "c", "a", "d", "e"), 2).distinct.collect.foreach(println)
    println(sc.parallelize(1 to 10).distinct(2).partitions.length)
    println(sc.parallelize(1 to 10).distinct(3).partitions.length)
  }

  @Test
  def test15(): Unit = {
    val rdd = sc.parallelize(List(1, 2, 3))
    println(rdd.dependencies)
    println(rdd.map(a => a).dependencies.length)
    println(rdd.cartesian(rdd).dependencies.length)
  }

  @Test
  def test14(): Unit = {
    sc.parallelize(List(1, 2, 3, 4, 5, 2, 3, 4, 1, 25, 5, 3)).countByValue().foreach(println)
  }

  @Test
  def test13(): Unit = {
    val rdd = sc.parallelize(List((1, "a"), (2, "b"), (1, "c"), (3, "d"), (2, "e")))
    rdd.countByKey().foreach(println)
    rdd.countByValue().foreach(println)
  }

  @Test
  def test12(): Unit = {
    println(sc.parallelize(Nil).context)
    println(sc.parallelize(Nil).sparkContext)
    println(sc.parallelize(List(1, 2, 3)).count)
  }

  @Test
  def test11(): Unit = {
    val rdd = sc.parallelize(List(("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5)))
    rdd.aggregateByKey(0)(_ + _, _ + _).foreach(println)
    rdd.combineByKey(List(_), (list: List[Int], value: Int) => value :: list, (list1: List[Int], list2: List[Int]) => list1 ::: list2).foreach(println)
    val a = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val b = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    val c = b.zip(a)
    val d = c.combineByKey(List(_), (x: List[String], y: String) => y :: x, (x: List[String], y: List[String]) => x ::: y)
    d.collect
  }

  @Test
  def test10(): Unit = {
    val rdd = sc.parallelize(List(1, 2, 1, 3))
    val rdd2: RDD[(Int, Int)] = rdd.zip(rdd)
    rdd2.collectAsMap().foreach(println)
  }

  @Test
  def test9(): Unit = {
    val rdd = sc.parallelize(List("a", "b", "c", "d", "e", "f"), 3)
    rdd.collect.foreach(print)
    println
    val rdd2: RDD[String] = rdd.collect { case item: String => item.toUpperCase }
    rdd2.foreach(println)
  }

  @Test
  def test8(): Unit = {
    val rdd: RDD[Int] = sc.parallelize(1 to 10, 5)
    rdd.saveAsTextFile("C:/a")
    rdd.coalesce(2).saveAsTextFile("C:/a2")
    println(rdd.partitions.length)
    println(rdd.coalesce(2).partitions.length)
    rdd.repartition(3).saveAsTextFile("C:/a3")
    println(rdd.partitions.length)
    println(rdd.repartition(3).partitions.length)
  }

  @Test
  def test7(): Unit = {
    sc.setCheckpointDir("checkpoint")
    val rdd: RDD[Int] = sc.parallelize(1 to 10)
    rdd.checkpoint()
    println(rdd.count)
  }

  @Test
  def test6(): Unit = {
    sc.parallelize(List(1, 2, 3)).cartesian(sc.parallelize(List(4, 5, 6))).foreach(println)
  }

  @Test
  def test5(): Unit = {
    val rdd = sc.parallelize(List(("a", 1), ("a", 3), ("b", 5), ("c", 7), ("a", 9), ("c", 11)), 2)
    rdd.aggregateByKey(0)(math.max(_, _), _ + _).foreach(println)
    rdd.aggregateByKey(100)(math.max(_, _), _ + _).foreach(println)
  }

  @Test
  def test4(): Unit = {
    val rdd1 = sc.parallelize(List("12", "23", "345", ""), 2)
    println(rdd1.aggregate("")((x, y) => math.max(x.length, y.length).toString, _ + _))
    val rdd2 = sc.parallelize(List("12", "23", "", "345"), 2)
    println(rdd2.aggregate("")((x, y) => math.max(x.length, y.length).toString, _ + _))
  }

  @Test
  def test3(): Unit = {
    val rdd: RDD[String] = sc.parallelize(List("12", "23", "345", "4567"), 2)
    rdd.mapPartitionsWithIndex {
      case (index, iter) => iter.toList.map(item => s"${index},${item}").iterator
    }.foreach(println)
    println(rdd.aggregate("")((x, y) => math.max(x.length, y.length).toString, _ + _))
    println(rdd.aggregate("")((x, y) => math.min(x.length, y.length).toString, _ + _))
  }

  @Test
  def test2(): Unit = {
    val rdd: RDD[String] = sc.parallelize(List("a", "b", "c", "d", "e", "f"), 2)
    rdd.mapPartitionsWithIndex {
      case (index, iter) => iter.toList.map(item => s"index:${index},item:${item}").iterator
    }.foreach(println)
    println(rdd.aggregate("")(_ + _, _ + _))
    println(rdd.aggregate("xyz")(_ + _, _ + _))
  }

  @Test
  def test(): Unit = {
    val rdd: RDD[Int] = sc.parallelize(List(1, 3, 5, 7, 9), 2)
    rdd.mapPartitionsWithIndex({
      case (index, iter) => iter.toList.map(item => s"index:${index},item:${item}").iterator
    }).foreach(println)
    println(rdd.aggregate(0)(Math.max(_, _), _ + _))
    println(rdd.aggregate(5)(Math.max(_, _), _ + _))
  }

  def fun(index: Int, iter: Iterator[Int]): Iterator[String] = {
    iter.toList.map(item => s"index:${index},item:${item}").iterator
  }

}
