package dse.search.training

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").getOrCreate()

    val dataFrameReader = spark.read

    val response = dataFrameReader
      .option("header", "false")
      .option("interSchema", value = true)
      .option("ignoreLeadingWhiteSpace","true")
      .option("ignoreTrailingWhiteSpace","true")
      .csv("in/RealEstate.csv")



    val noOfResponses = response.count.toInt


    // Rename Column names
    val renamedColumn = Seq("MLS", "Location", "Price", "Bedrooms", "Bathrooms", "Size", "Price SQ Ft", "Status")

    // Change column data type
//    response.withColumn("_c2", response.col("_c2").cast("double"))
//    response.withColumn("_c3", response.col("_c3").cast("integer"))
//    response.withColumn("_c4", response.col("_c4").cast("integer"))
//    response.withColumn("_c5", response.col("_c5").cast("double"))
//    response.withColumn("_c6", response.col("_c6").cast("double"))

    response.printSchema()

    // show all columns
    response.toDF(renamedColumn: _*).show(noOfResponses)

    // number of houses located in Santa Maria-Orcutt
    housesInSMO(response, noOfResponses)

    // List the location and price of houses which are priced over 500000
    housesOver50k(response, noOfResponses)

    // List the houses which have 3 bedrooms and available for short sale\
    housesWhichHas3BedRooms(response, noOfResponses)

    cayucosHighestPriceHouse(response)

    // average price of an each city
    avgPriceInEachCity(response, noOfResponses)

    // Get average price
    avgPriceofHouse(response, noOfResponses)
  }

  private def avgPriceInEachCity(response: DataFrame, noOfResponses: Int) = {
    // average price of an each city
    val avgPriceInEachCity = response.withColumn("_c2", response.col("_c2").cast("double")).groupBy("_c1").avg("_c2")
    avgPriceInEachCity.show(noOfResponses)
  }

  private def avgPriceofHouse(response: DataFrame, noOfResponses: Int) = {
    // Get average price
    response.select("_c5").withColumn("_c5", response.col("_c5").cast("double"))
    response.select("_c6").withColumn("_c6", response.col("_c6").cast("double"))

    response.select("*")
      .filter((response.col("_c2")
        .+(response.col("_c6"))
        .*(response.col("_c5")))./(2))
      .show(noOfResponses)
  }

  private def cayucosHighestPriceHouse(response: DataFrame) = {
    // Highest priced house in Cayucos which has more than 3 bedrooms and 2 bathrooms
    val cayucosHighestPriceHouse = response.select("*").filter(response.col("_c1").===("Cayucos") && response.col("_c3") > 3 && response.col("_c4") > 2).groupBy("_c2").max()
    cayucosHighestPriceHouse.show()
  }

  private def housesWhichHas3BedRooms(response: DataFrame, noOfResponses: Int) = {
    // List the houses which have 3 bedrooms and available for short sale\
    val housesWhichHas3BedRooms = response.select("*").filter(response.col("_c3").===(3) && response.col("_c7").===("Short Sale"))
    housesWhichHas3BedRooms.show(noOfResponses)
  }

  private def housesOver50k(response: DataFrame, noOfResponses: Int) = {
    // List the location and price of houses which are priced over 500000
    val housesOver50k = response.select("_c1", "_c2").filter(response.col("_c2") > 500000)
    housesOver50k.show(noOfResponses)
  }

  private def housesInSMO(response: DataFrame, noOfResponses: Int) = {
    // number of houses located in Santa Maria-Orcutt
    val housesInSMO = response.groupBy("_c1").count().filter(response.col("_c1").===("Santa Maria-Orcutt"))
    housesInSMO.show(noOfResponses)
  }
}
