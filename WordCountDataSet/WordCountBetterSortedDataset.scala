package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountBetterSortedDataset {

  case class Book(value: String)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // 로컬의 모든 코어 사용하여 SparkSession 생성
    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    // 각 line을 Dataset으로 읽어들임
    import spark.implicits._
    val input = spark.read.text("data/book.txt").as[Book]

    // 정규식을 사용하여 특수문자 제거 및 단어 추출
    val words = input
      .select(explode(split($"value", "\\W+")).alias("word"))
      .filter($"word" =!= "")

    // 소문자로 정규화
    val lowercaseWords = words.select(lower($"word").alias("word"))

    // 동일한 단어들 count
    val wordCounts = lowercaseWords.groupBy("word").count()

    // count로 정렬
    val wordCountsSorted = wordCounts.sort("count")

    wordCountsSorted.show(wordCountsSorted.count.toInt)
  }
}

