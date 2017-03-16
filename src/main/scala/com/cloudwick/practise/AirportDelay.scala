package com.cloudwick.practise


import org.apache.spark._

/**
 * Created by Rajiv on 3/16/17.
 */
object AirportDelay {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Compute Airport Delays").setMaster("local")
    val sc = new SparkContext(conf)

    val inputPath = args(0);
    val outputPath = args(1);
    val inputRDD = sc.textFile(inputPath)
    val header = inputRDD.first()

    val FilteredRDD = inputRDD.filter(row => row != header)

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
    val InputMapRDD = FilteredRDD.map { line =>
      val cols = line.split(",")
      AirportSchema(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(3).toInt, cols(4), cols(5).toInt, cols(6), cols(7).toInt, cols(8), cols(9).toInt,
        cols(10), cols(11), cols(12), cols(13), cols(14), cols(15), cols(16), cols(17), cols(18).toInt)
    }



  }

}
