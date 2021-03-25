package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

/** 연령 별 평균 친구 수를 계산 */
object FriendsByAge {
  
  /** 입력받은 값을 (age, numFriends) 튜플로 가공. */
  def parseLine(line: String): (Int, Int) = {
      val fields = line.split(",")
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      (age, numFriends)
  }
  
  def main(args: Array[String]) {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // SparkContext 생성
    val sc = new SparkContext("local[*]", "FriendsByAge")
  
    // 데이터를 RDD에 Load
    val lines = sc.textFile("data/fakefriends-noheader.csv")
    
    // parseline 함수로 (age, numFriends) 튜플로 변환
    val rdd = lines.map(parseLine)
    
    // 먼저 연령 별 total를 계산하기 위해 (age, numFriends)를 (age, (numFriends, 1))로 변환
    // age가 같은 튜플들의 (numFriends,1)을 모두 합계
    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
    
    // (age, (totalFriends, totalInstances)) 튜플을 가지고 있음
    // 평균을 계산하기 위해 totalFriends를 totalInstances로 나눔
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
    
    val results = averagesByAge.collect()
    
    // 정렬하여 결과 출력
    results.sorted.foreach(println)
  }
    
}
  