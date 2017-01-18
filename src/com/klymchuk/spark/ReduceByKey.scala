package com.klymchuk.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/**
  * Created by iklymchuk on 1/18/17.
  */
object ReduceByKey extends Serializable{

  def extractCustomerPricePairs(line: String) = {

    val fields = line.split(",")
    val id = fields(0).toInt
    val value = fields(2).toFloat
    (id, value)
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "ReduceByKey")

    val lines = sc.textFile("inputData/customer-orders.csv")

    val rdd = lines.map(extractCustomerPricePairs)

    val totalByCustomer = rdd.reduceByKey( (x,y) => x + y )

    val results = totalByCustomer.collect()

    //results.foreach(println)

    //*********SORTED OUTPUT**************
    results.sorted.foreach(println)
  }

}
