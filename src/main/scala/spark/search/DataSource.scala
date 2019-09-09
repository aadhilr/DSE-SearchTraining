package spark.search

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

object DataSource {


  val pageTableColumnCount = 14
  val categoryTableColumnCount = 7

  val pageLinkFilePath = "in/Page.text"
  val categoryLinkFilePath = "in/CategoryLink.text"

  /**
   *
   * @param table table name
   * @return length of
   */
  def getStartPoint(table: String): Int = {
    s"INSERT INTO '$table' VALUES (".length
  }

  /**
   * Read all page metadata in sql dump
   * @return Extracted page data
   */
  def readPages(implicit spark: SparkSession): Dataset[Page] = {
    import spark.implicits._
    val page = spark.read.textFile(pageLinkFilePath)
    val pageTableName = "page"
    val p = processFile(page,pageTableName, spark, pageTableColumnCount)
    p.map(r => Page(r.head, r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12), r(13)))
  }

  /**
   * Read all category metadata in sql dump
   * @return Extracted category data
   */
  def readCategories(implicit spark: SparkSession): Dataset[CategoryLink] = {
    import spark.implicits._
    val category = spark.read.textFile(categoryLinkFilePath)
    val categoryLinksTableName = "categorylinks"
    val c = processFile(category,categoryLinksTableName, spark, categoryTableColumnCount)
    c.map(r => CategoryLink(r.head, r(1), r(2), r(3), r(4), r(5), r(6)))
  }


  /**
   * Process the given file and extract the values to process
   * @param response file to read
   * @param table table name to read
   * @param spark spark session
   * @param columnCount column count of a table
   * @return List of Strings of dataset
   */
  private def processFile(response: Dataset[String], table: String, spark: SparkSession, columnCount: Int): Dataset[List[String]] = {
    import spark.implicits._
    val processStr = response.filter(r =>
      r.startsWith("INSERT")
    ).flatMap(l => {
      l.substring(getStartPoint(table), (l.length - 2))
        .split("\\),\\(")
    })
      .map(_.split(",").toList).filter(_.size == columnCount )

    processStr

  }

}

// Page case class
case class Page(id: String, namespace: String, title: String, restrictions: String, counter: String, is_redirect: String, is_new: String, random: String, touched: String, links_updated: String, latest: String, len: String, content_model: String, lang: String)


// Category Link case class
case class CategoryLink(from: String, to: String, sortkey: String, timestamp: String, sortkey_prefix: String, collation: String, `type`: String)
