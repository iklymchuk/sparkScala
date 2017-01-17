package com.klymchuk.spark

import org.apache.log4j._
import org.apache.spark._

/**
  * Created by iklymchuk on 1/17/17.
  */
object FlatMap {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FlatMap")

    // Read each line of my book into an RDD
    val input = sc.textFile("inputData/book.txt")

    //***************FIRST ONE******************************
    // Split into words separated by a space character
    //val words = input.flatMap(x => x.split(" "))

    //***************SECOND ONE******************************
    // Split into words separated by a space character
    val words = input.flatMap(x => x.split("\\W+"))
    val lowercaseWords = words.map(x => x.toLowerCase())

    // Count up the occurrences of each word
    val wordCounts = lowercaseWords.countByValue()

      // Print the results.
    wordCounts.foreach(println)
  }

}
