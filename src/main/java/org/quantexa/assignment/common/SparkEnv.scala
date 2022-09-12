package org.quantexa.assignment.common

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparkEnv extends App {
  lazy val spark = getSparkSession

    def getSparkSession : SparkSession={
      SparkSession.builder()
        .master("local[*]")
        .appName("Quantexa-Assignment")
        .getOrCreate()
    }

  def readCSVFile(file :String,schemaDDL : String) : DataFrame={

    val csvOptions = Map("header"->"true",
                          "delimiter"->",",
                          "dateFormat"->"yyyy-MM-dd")

    spark.read
      .format("csv")
      .options(csvOptions)
      .schema(StructType.fromDDL(schemaDDL))
      .load(file)
  }

}
