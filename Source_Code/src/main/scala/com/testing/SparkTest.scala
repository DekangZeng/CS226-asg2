package com.testing

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]) {
    val writer_1= new BufferedWriter(new FileWriter(new File("task1.txt")))
    val writer_2= new BufferedWriter(new FileWriter(new File("task2.txt")))
    val logFile = args(0)// Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val rdd = new SparkContext(conf).textFile(logFile)
    val result=rdd.map(line=>line.split("\t"))
      .map(x=>(x(5).toInt,(x(6).toFloat,1)))
      .reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
      .map(x=>(x._1,x._2._1/x._2._2))
    result.collect().sortBy(x=>x._1).foreach(line=> {
      val str = "Code " + line._1 + ", average number of bytes = " + line._2+"\n"
      writer_1.write(str)
    })
    writer_1.close()
    val rdd2 = rdd
    val a=rdd2.map(line=>line.split("\t"))
      .map(x=>((x(0),x(4)),(x(2).toInt,x(5).toInt,x(6).toInt)))
    val b=rdd.map(line=>line.split("\t"))
      .map(x=>((x(0),x(4)),(x(2).toInt,x(5).toInt,x(6).toInt)))
      .join(a).filter( line => {
      ((line._2._1._1 - line._2._2._1).abs <= 3600)
        line._2._1!=line._2._2
      }).distinct().collect()
    b.foreach(println)
    for(a<-b) {

      val output = "(\"" + a._1._1 + "\",\"-\"," + a._2._1._1 + ",\"GET\"," + a._1._2 + "," + a._2._1._2 + "," + a._2._1._3 + ")\t" + "(\"" + a._1._1 + "\",\"-\"," + a._2._2._1 + ",\"GET\"," + a._1._2 + "," + a._2._2._2 + "," + a._2._2._3 + ")\n"
      writer_2.write(output)
    }
    writer_2.close()
  }
}