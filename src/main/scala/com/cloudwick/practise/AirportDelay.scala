package com.cloudwick.practise

import java.util.{Calendar}
import com.typesafe.config._
import org.apache.spark._
import org.apache.spark.rdd.RDD

/**   * Created by Rajiv on 3/16/17.  */


object AirportDelay {

  def main(args: Array[String]) {

  val conf = new SparkConf().setAppName("Compute Airport Delays")
  val sc = new SparkContext(conf)
  val configParam = ConfigFactory.load()

  //val inputPath = args(0);
  val inputPath = "hdfs://10.2.2.28:8020/airportInput"
  // Reading the file
  val inputRDD = sc.textFile(inputPath)
  // Getting the header record of the file
  val header = inputRDD.first()
  val FilteredRDD = inputRDD.filter(row => row != header)
  val city = configParam.getString("MyApp.airport")

  //Desired Input = "San Francisco International"
  //val citiesFilePath = sc.textFile(args(1))
  val citiesFilePath = sc.textFile("hdfs://10.2.2.28:8020/airportList")
  val airport = getAirport(city, citiesFilePath)
  // Get Fully qualified Airport Name
  //val carrierList = sc.textFile(args(2))
  val carrierList = sc.textFile("hdfs://10.2.2.28:8020/carrierList")
  val carrierInfo = cleanCarrierFile(carrierList)

  // Defining case class

  case class AirportSchema(Year: Int,
                           Month: Int,
                           DayofMonth: Int,
                           DayOfWeek: Int,
                           DepTime: String,
                           CRSDepTime: Int,
                           ArrTime: String,
                           CRSArrTime: Int,
                           UniqueCarrier: String,
                           FlightNum: Int,
                           TailNum: String,
                           ActualElapsedTime: String,
                           CRSElapsedTime: String,
                           AirTime: String,
                           ArrDelay: String,
                           DepDelay: String,
                           Origin: String,
                           Dest: String,
                           Distance: Int
                            )

  // Attaching Schema to the file
  val inputMapRDD = FilteredRDD.map { line =>
    val cols = line.split(",")
    AirportSchema(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toInt, cols(4), cols(5).toInt, cols(6), cols(7).toInt, cols(8), cols(9).toInt,
      cols(10), cols(11), cols(12), cols(13), cols(14), cols(15), cols(16), cols(17), cols(18).toInt)
  }


  val arrDelayRDD = inputMapRDD.filter(row => (airport == row.Dest && ((row.ArrDelay != "NA") && (row.ArrDelay).toInt > 0)))
  val depDelayRDD = inputMapRDD.filter(row => (airport == row.Origin && ((row.DepDelay != "NA") && (row.DepDelay).toInt > 0)))
  val arrDelayGroupRDD = arrDelayRDD.map(row => (row.Year, row.UniqueCarrier, getWeekOfYear(row.Year, row.Month, row.DayofMonth), row.ArrDelay.toInt))
  val depDelayGroupRDD = depDelayRDD.map(row => (row.Year, row.UniqueCarrier, getWeekOfYear(row.Year, row.Month, row.DayofMonth), row.DepDelay.toInt))
  val totalDelayRDD = arrDelayGroupRDD.union(depDelayGroupRDD).map(row => ((row._1, row._2, row._3), row._4)).reduceByKey(_ + _).sortByKey()

  val GroupedDelayRDD = totalDelayRDD.map(row => (row._1._2, (row._1._1, row._1._2, row._1._3, (row._2.toFloat) / 60)))
  val myOutput = GroupedDelayRDD.join(carrierInfo).sortByKey().map(row => (row._2._1._1, row._1, row._2._2, row._2._1._3, row._2._1._4))

  //myOutput.take(200).foreach(println)

  myOutput.saveAsTextFile("hdfs://10.2.2.28:8020/AirOutput")


}

  //Function returning Airport of a city
  def getAirport(city: String, citiesFile: RDD[String]): String = {

    val header = citiesFile.first()
    val fileData = citiesFile.filter(row => row != header)
    val fileMapping = fileData.map { lines =>
      val cols = lines.split(",")
      (cols(0), cols(1))
    }
    // Removing Extra Quotes on the String values
    val filteredAirport = fileMapping.filter(row => row._2.substring(1, row._2.length() - 1) == city)
    val myAirports = filteredAirport.map(row => row._1.substring(1, row._1.length() - 1))

    return myAirports.first()

  }


  def getWeekOfYear(Year: Int, Month: Int, Day: Int): Int = {

    val calendarObject = Calendar.getInstance();
    calendarObject.set(Year, Month - 1, Day)
    return calendarObject.get(Calendar.WEEK_OF_YEAR)
  }


  def cleanCarrierFile(carrierList: RDD[String]): RDD[(String, String)] = {

    val carrierHeader = carrierList.first()
    val carrierData = carrierList.filter(row => row != carrierHeader)
    val fileMapping = carrierData.map { lines =>
      val cols = lines.replace("\"", "").split(",")
      (cols(0), cols(1))
    }

    return fileMapping

  }


}

