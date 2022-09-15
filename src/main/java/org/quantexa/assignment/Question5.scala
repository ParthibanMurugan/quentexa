package org.quantexa.assignment

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, count, lit}
import org.quantexa.assignment.Question4.{flightData, joinColumn, rows}
import org.quantexa.assignment.common.SparkEnv

object Question5 extends SparkEnv{

  def filterDataByDate(data : DataFrame,fromDate : String,toDate : String) : DataFrame={

    data.filter(s"date >= to_date('$fromDate','yyyy-MM-dd') AND date <= to_date('$toDate','yyyy-MM-dd') ")
  }

  var data  = filterDataByDate(flightData,"2017-01-02","2017-05-30")
  var joinColumn = Seq("flightId", "from", "to", "date")
  val rows = "3"

  data = data.as("a").join(data.as("b"), joinColumn)
    .where(col("a.passengerId").<(col("b.passengerId")))
    .select(col("b.passengerId").as("pass_1"), col("a.*"))

  data = data.withColumnRenamed("passengerId", "pass_2")
    .groupBy("pass_1", "pass_2")
    .agg(count(lit(1)).as("numOfFlights"), functions.min("date").as("From"), functions.max("date").as("To"))
    .filter(s"numOfFlights > $rows")
    .orderBy(col("numOfFlights").desc)

  data.show(false)
  saveAsCsv(data,"src/main/resources/passengerTravelTogatherByDate")

}
