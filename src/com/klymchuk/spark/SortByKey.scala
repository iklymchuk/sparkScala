package com.klymchuk.spark

import org.apache.spark._
import org.apache.log4j._

/**
  * Created by iklymchuk on 1/18/17.
  */
object SortByKey extends Serializable{

  def extractCustomerPricePairs(line: String) = {
    var fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "SortByKey")

    val input = sc.textFile("inputData/customer-orders.csv")

    val rdd = input.map(extractCustomerPricePairs)

    val totalByCustomer = rdd.reduceByKey( (x,y) => x + y )

    val flipped = totalByCustomer.map( x => (x._2, x._1) )

    val totalByCustomerSorted = flipped.sortByKey()

    val results = totalByCustomerSorted.collect()

    results.foreach(println)

  }
}
