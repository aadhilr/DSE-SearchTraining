package dse.search.training

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").getOrCreate()

    import spark.implicits._
    val data = Seq("a", "b")

    val dataFrame = spark.sparkContext.parallelize(data).toDF

    println("Welcome to DSE Search")

    dataFrame.show(100, false)
  }

}
