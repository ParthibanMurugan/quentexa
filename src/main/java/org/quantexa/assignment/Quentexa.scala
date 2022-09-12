package org.quantexa.assignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.quantexa.assignment.common.SparkEnv
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object Quentexa extends SparkEnv {
  val FILGHT_CSV = "src/main/resources/flightData.csv"
  val flightSchema = "passengerId INT,flightId INT,from STRING,to STRING,date DATE"

  val PASSANGER_CSV = "src/main/resources/passengers.csv"
  val passangerSchema = "passengerId INT,firstName STRING,lastName STRING"

  val filghtData = readCSVFile(FILGHT_CSV,flightSchema)
  val passangerData = readCSVFile(PASSANGER_CSV,passangerSchema)

  val util = new ProcessUtil(spark)
  //Question:- 1
  //Find the total number of flights for each month.
  val flightCountDF = util.getFlightsForEachMonth(filghtData)
  flightCountDF.show(false)
  util.saveAsCsv(flightCountDF,"src/main/resources/flightCount")

  /*
  +-----+-----+
  |month|count|
  +-----+-----+
  |01   |97   |
  |12   |94   |
  |05   |92   |
  |04   |92   |
  |07   |87   |
  |09   |85   |
  |03   |82   |
  |10   |76   |
  |08   |76   |
  |11   |75   |
  |02   |73   |
  |06   |71   |
  +-----+-----+

   */

  //Question:2
  //Find the names of the 100 most frequent flyers
  val mostFlightDF = util.fetchMostFlightPassengerInfo(filghtData,passangerData)
  mostFlightDF.show(false)
  util.saveAsCsv(mostFlightDF,"src/main/resources/most_flight_info")
  /*
  +---------------+-----------+---------+--------+
  |NumberOfFlights|passengerId|firstName|lastName|
  +---------------+-----------+---------+--------+
  |32             |2068       |Yolande  |Pete    |
  |27             |1677       |Katherina|Vasiliki|
  |27             |4827       |Jaime    |Renay   |
  |26             |8961       |Ginny    |Clara   |
  |26             |3173       |Sunshine |Scott   |
  |25             |5867       |Luise    |Raymond |
  |25             |2857       |Son      |Ginette |
  |25             |760        |Vernia   |Mui     |
  |25             |8363       |Branda   |Kimiko  |
  |25             |5096       |Blythe   |Hyon    |
  |25             |6084       |Cole     |Sharyl  |
  |25             |288        |Pamila   |Mavis   |
  |25             |917        |Anisha   |Alaine  |
  |24             |1240       |Catherine|Missy   |
  |24             |5668       |Gladis   |Earlene |
  |24             |1343       |Bennett  |Staci   |
  |24             |2441       |Kayla    |Rufus   |
  |24             |3367       |Priscilla|Corie   |
  |23             |613        |Palmer   |Yuonne  |
  |23             |211        |Lori     |Jeremiah|
  +---------------+-----------+---------+--------+

   */

  //Question: 3
  //Find the greatest number of countries a passenger has been in without being in the UK.
  // For example, if the countries a passenger was in were: UK -> FR -> US -> CN -> UK -> DE -> UK, the correct answer would be 3 countries.

   val longTripDF = util.longestJourney(filghtData,"uk",3)
   longTripDF.show(false)
  util.saveAsCsv(longTripDF,"src/main/resources/longest_journey")


  /*
  +-----------+--------------------------------+-------+
  |passengerId|from_list                       |maxTrip|
  +-----------+--------------------------------+-------+
  |15157      |[be, se, tk]                    |3      |
  |5061       |[cn, no, iq]                    |3      |
  |11024      |[pk, no, fr]                    |3      |
  |14030      |[be, nl, us]                    |3      |
  |4859       |[cn, pk, se]                    |3      |
   */


  //Question:4
  //Find the passengers who have been on more than 3 flights together.
  val passengerTravelDF = util.fetchPassengerTravelTogather(filghtData,3)
  passengerTravelDF.show(false)
  util.saveAsCsv(passengerTravelDF,"src/main/resources/passengerTravelTogather")
  /*
  +------+------+------------+----------+----------+---+
  |pass_1|pass_2|numOfFlights|From      |To        |rn |
  +------+------+------------+----------+----------+---+
  |701   |760   |15          |2017-01-02|2017-05-30|1  |
  |3503  |3590  |14          |2017-01-15|2017-06-24|2  |
  |2717  |2759  |14          |2017-01-10|2017-05-03|3  |
  +------+------+------------+----------+----------+---+
   */
  /*Get Passnger details by date filter*/
  val passengerTraveByDateDF = util.flownTogether(filghtData,3,"2017-01-02","2017-05-30")
  passengerTraveByDateDF.show(false)
  util.saveAsCsv(passengerTraveByDateDF,"src/main/resources/passengerTravelTogatherByDate")
  /*
  +------+------+------------+----------+----------+---+
  |pass_1|pass_2|numOfFlights|From      |To        |rn |
  +------+------+------------+----------+----------+---+
  |701   |760   |15          |2017-01-02|2017-05-30|1  |
  |2717  |2759  |14          |2017-01-10|2017-05-03|2  |
  |2939  |4395  |12          |2017-02-07|2017-05-19|3  |
  +------+------+------------+----------+----------+---+
   */



}
