package org.quantexa.assignment

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.quantexa.assignment.common.SparkEnv

object Question2 extends SparkEnv{

  val spec = Window.orderBy(col("count").desc)
  val fDf = flightData.groupBy(col("passengerId"))
    .count
    .withColumn("row_num", row_number().over(spec))
    .limit(100)

  val joinColumn = Seq("passengerId")

  val data = passengerData.join(fDf, joinColumn)
    .select(fDf.col("count").as("NumberOfFlights"),
      passengerData.col("passengerId"),
      passengerData.col("firstName"),
      passengerData.col("lastName"))

  data.show(false)

  saveAsCsv(data,"src/main/resources/most_flight_info")

}
