package doric

import java.util.TimeZone
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.SparkSession

object DocInit {
  def getSpark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val timeZone: String = "UTC"
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone))

    val ss = SparkSession
      .builder()
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.session.timeZone", timeZone)
      .config("spark.sql.datetime.java8API.enabled", value = true)
      .appName("spark session")
      .getOrCreate()

    ss.sparkContext.setLogLevel("ERROR")
    ss
  }
}
