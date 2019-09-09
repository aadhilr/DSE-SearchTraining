package spark.search

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SparkSearch {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local").getOrCreate()

    import spark.implicits._

    val kohlsBrands = Seq("'AfghanistanTransportations'","'Anarchism'", "'AfghanistanHistory'", "'AfghanistanPeople'", "'AfghanistanCommunications'").toDS()

    val readPages = DataSource.readPages(spark)
    val readCategory = DataSource.readCategories(spark)


    // get ID for the given kohls brands
    val kohlsBrandID = readPages.join(kohlsBrands, readPages("title") === kohlsBrands("value"))
      .select("id", "title")

    // get category for the kohls brands ID
    val kohlsBrandsCat= kohlsBrandID.join(readCategory, kohlsBrandID("id") === readCategory("from"))
      .select("id", "title", "to")

    // get the to value for the kohls brands with the from value
    val kohlsBrandTo = kohlsBrandsCat.join(readCategory, kohlsBrandsCat("to") === readCategory("to"))
        .select(col("from"), col("title")
        .as("brands"))

    // Find alternate brands for the kohls brands
    val result = kohlsBrandTo.join(readPages, col("from") === col("id"))
      .distinct()
      .select(col("id"), col("title"), col("brands"))
      .groupBy("brands")
      .agg(collect_list("title")
        .as("Alternate Brands"))

    result.show(result.count.toInt, false)
  }
}

