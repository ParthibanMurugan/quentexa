package org.quantexa.assignment

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.util._


import java.time.LocalDate

class ProcessUtil (spark : SparkSession){
//select month,count(distinct flightId) as count
// from flights_data
// group by month
// order by count desc
  def getFlightsForEachMonth(data : DataFrame) : DataFrame ={
      data.withColumn("month",date_format(col("date"),"MM"))
        .select(col("flightId"),col("month"))
        .distinct()
        .groupBy(col("month"))
        .count().as("count")
        .orderBy(col("count").desc)
  }



  def fetchMostFlightPassengerInfo(flightData : DataFrame,passengerData : DataFrame) : DataFrame={

    val spec = Window.orderBy(col("count").desc)
    val fDf = flightData.groupBy(col("passengerId"))
      .count
      .withColumn("row_num",row_number().over(spec))
      .filter("row_num <= 100")

    passengerData.join(fDf,passengerData.col("passengerId").equalTo(fDf.col("passengerId")))
      .select(fDf.col("count").as("NumberOfFlights"),
        passengerData.col("passengerId"),
        passengerData.col("firstName"),
        passengerData.col("lastName"))
  }

  def flownTogether(flightData : DataFrame, rows : Int, fromDate : String, toDate : String) : DataFrame={

   val data = flightData.filter(s"date >= to_date('$fromDate','yyyy-MM-dd') AND date <= to_date('$toDate','yyyy-MM-dd') ")
    //val data = flightData.filter(s"date >= $fromDate AND date <= $toDate")

    fetchPassengerTravelTogather(data,rows)
  }

  def fetchPassengerTravelTogather(flightData : DataFrame,rows : Int) : DataFrame={

    flightData.createOrReplaceTempView("flights_data")

    var data = spark.sql("SELECT a.passengerId as pass_1,b.* FROM flights_data a" +
      " INNER JOIN flights_data b ON a.passengerId < b.passengerId AND a.flightId = b.flightId " +
      "AND a.from = b.from AND a.to = b.to AND a.date = b.date")

    val windowSpec = Window.orderBy(col("numOfFlights").desc)

    data.withColumnRenamed("passengerId","pass_2")
      .groupBy("pass_1","pass_2")
      .agg(count(lit(1)).as("numOfFlights"),min("date").as("From"),max("date").as("To"))
     // .withColumn("rn",row_number().over(windowSpec))
      .filter(s"numOfFlights > $rows")
      .orderBy(col("numOfFlights").desc)
  }

  def longestJourney(flightData : DataFrame,skipCountry : String,numOccr :Int) : DataFrame={
    import scala.collection.mutable._
    import org.apache.spark.sql._
    val maxTrip = udf((data :Seq[String])=>{
      var size = 0;
      var list = data.toList
      while(list.contains(skipCountry)){
        list.contains(skipCountry) match {
          case true => {
            val idx = list.indexOf(skipCountry)
            if(idx > size){
              size = idx
            }
            list = list.splitAt(idx+1)._2
          }
          case _ => if(list.size > size){
            size = list.size
          }
        }
      }

      if (list.size > size) {
        size = list.size
      }

      size

    })
    val spec = Window.partitionBy("passengerId").orderBy(col("date").asc)
    flightData.withColumn("rn",row_number().over(spec))
      .groupBy("passengerId")
      .agg(collect_list("from").as("from_list"))
      .withColumn("maxTrip",maxTrip(col("from_list")))
      .filter(s"maxTrip >=$numOccr")
      .drop("from_list")
      .orderBy(col("maxTrip").desc)
  }

  def saveAsCsv(dataset : DataFrame,fileName : String): Unit={
    import org.apache.spark.sql._
    dataset.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .csv(fileName)
  }
}
