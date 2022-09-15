package org.quantexa.assignment

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, lit, row_number, udf}
import org.quantexa.assignment.common.SparkEnv

object Question3 extends SparkEnv{
  val skipCountry = "uk"
  val numOccr = "3"

  import scala.collection.mutable._
  import org.apache.spark.sql._

  val maxTrip = udf((data: Seq[String], country: String) => {
    var size = 0;
    var list = data.toList
    while (list.contains(country)) {
      list.contains(country) match {
        case true => {
          val idx = list.indexOf(country)
          if (idx > size) {
            size = idx
          }
          list = list.splitAt(idx + 1)._2
        }
        case _ => if (list.size > size) {
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
  var data = flightData.withColumn("rn", row_number().over(spec))
    .groupBy("passengerId")
    .agg(collect_list("from").as("from_list"))
    .withColumn("maxTrip", maxTrip(col("from_list"), lit(skipCountry)))
    .filter(s"maxTrip >=$numOccr")
    .drop("from_list")
    .orderBy(col("maxTrip").desc)

  data.show(false)
  saveAsCsv(data,"src/main/resources/longest_journey")
}
