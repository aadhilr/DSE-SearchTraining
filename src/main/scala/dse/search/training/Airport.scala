package dse.search.training

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Airport {

  val conf = new SparkConf().setAppName("").setMaster("local[*]")

  val sc = new SparkContext(conf)

  val response = sc.textFile("in/airports.text")

  val COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Count the #ofAirports whose latitude are greater than 40
    countOfAirportsInIreLand()

    // Count the #ofAirports whose latitude are greater than 40
    countLatitudeGreaterThan40()

    // Airports which are located in US
    airportsWhichAreLocatedInUS()

    // Airports in each country
    airportsInEachCountry()

  }

  private def airportsInEachCountry() = {
    // Airports in each country
    val airportsInEachCountry = response.map(airport => (airport.split(COMMA_DELIMITER)(3), airport.split(COMMA_DELIMITER)(1)))

    val airportsPerCountry = airportsInEachCountry.groupByKey()

    for ((country, airportName) <- airportsPerCountry.collectAsMap()) println(country + ": " + airportName.toList)
  }

  private def airportsWhichAreLocatedInUS() = {
    // Airports which are located in US
    val airportsWhichAreLocatedInUS = response.filter(line => line.split(COMMA_DELIMITER)(3) == "\"United States\"")

    // output the airport's name and the airport's city to “airports_in_usa.text”
    val airportsWhichAreLocatedInUStoFIle = airportsWhichAreLocatedInUS.map(line => {
      val splits = line.split(COMMA_DELIMITER)
      splits(1) + ": " + splits(2)
    })
    airportsWhichAreLocatedInUStoFIle.saveAsTextFile("out/airports_in_usa.text")
  }

  private def countLatitudeGreaterThan40() = {
    // Count the #ofAirports whose latitude are greater than 40
    val countLatitudeGreaterThan40 = response.filter(line => line.split(COMMA_DELIMITER)(6).toFloat > 40)
    println("countLatitudeGreaterThan40: " + countLatitudeGreaterThan40.count())

    // output the airport's name and the airport's latitude to “airports_by_latitude.text”
    val countLatitudeGreaterThan40ToFile = countLatitudeGreaterThan40.map(line => {
      val splits = line.split(COMMA_DELIMITER)
      splits(2) + ": " + splits(7)
    })
    countLatitudeGreaterThan40ToFile.saveAsTextFile("out/airports_by_latitude.text")
  }

  private def countOfAirportsInIreLand() = {
    // Calculate the airports located in ireland
    val countOfAirportsInIreLand = response.filter(line => line.split(COMMA_DELIMITER)(3) == "\"Ireland\"")
    println("countOfAirportsInIreLand: " + countOfAirportsInIreLand.count())
  }
}
