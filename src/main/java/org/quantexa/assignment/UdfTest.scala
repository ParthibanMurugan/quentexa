package org.quantexa.assignment

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.StructType
import org.quantexa.assignment.common.SparkEnv

import scala.collection.mutable.Seq

object UdfTest extends SparkEnv {

  val maxTrip = udf((data: Seq[String],skipCountry :String) => {
    var size = 0;
    //val skipCountry = "UK"
    var list = data.toList
    while (list.contains(skipCountry)) {
      list.contains(skipCountry) match {
        case true => {
          val idx = list.indexOf(skipCountry)
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

  val data = Seq(Row(List("UK","FR","US","UN","UK","DE","UK")),Row(List("UK","FR","US","UN","IND","DE","UK")))

  import spark.implicits._

  var df = spark.createDataFrame(spark.sparkContext.parallelize(data),StructType.fromDDL("country ARRAY<STRING>"))

  df.withColumn("maxTrip",maxTrip(col("country"),lit("UK")))
    .show(false)
}
