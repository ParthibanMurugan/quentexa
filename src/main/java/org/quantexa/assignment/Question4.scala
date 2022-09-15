package org.quantexa.assignment

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, count, lit}
import org.quantexa.assignment.common.SparkEnv

object Question4 extends SparkEnv{
  val joinColumn = Seq("flightId", "from", "to", "date")
  val rows = "3"

  val data = flightData.as("a").join(flightData.as("b"), joinColumn)
    .where(col("a.passengerId").<(col("b.passengerId")))
    .select(col("a.*"),col("b.passengerId").as("pass_1"))
    .withColumnRenamed("passengerId", "pass_2")
    .groupBy("pass_1", "pass_2")
    .agg(count(lit(1)).as("numOfFlights"), functions.min("date").as("From"), functions.max("date").as("To"))
    .filter(s"numOfFlights > $rows")
    .orderBy(col("numOfFlights").desc)

  data.show(false)
  saveAsCsv(data,"src/main/resources/passengerTravelTogather")

}
