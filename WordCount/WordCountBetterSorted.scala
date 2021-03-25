package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object WordCountBetterSorted {
 
  def main(args: Array[String]) {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local", "WordCountBetterSorted")
    
    val input = sc.textFile("data/book.txt")
    
    // 정규식으로 특수문자 제거
    val words = input.flatMap(x => x.split("\\W+"))
    
    // 소문자로 정규화
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // 각 단어 Count
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    
    // (word, count)를 뒤집어서 (count, word)로 변환
    // count를 키값으로 정렬
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()
    
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }
    
  }
  
}

