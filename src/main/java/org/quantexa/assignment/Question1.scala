package org.quantexa.assignment

import org.apache.spark.sql.functions.{col, date_format}
import org.quantexa.assignment.Quentexa.{flightCountDF, util}
import org.quantexa.assignment.common.SparkEnv

object Question1 extends SparkEnv{
  val data = flightData.withColumn("month", date_format(col("date"), "MM"))
    .select(col("flightId"), col("month"))
    .distinct()
    .groupBy(col("month"))
    .count().as("count")
    .orderBy(col("count").desc)

  data.show(false)
  saveAsCsv(data, "src/main/resources/flightCount")

}
