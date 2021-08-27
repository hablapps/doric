package doric

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.TimeZone

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val timeZone: String = "UTC"
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone))

    SparkSession
      .builder()
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.session.timeZone", timeZone)
      .appName("spark session")
      .getOrCreate()
  }

}
