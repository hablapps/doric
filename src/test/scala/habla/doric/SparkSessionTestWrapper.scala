package habla.doric

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    SparkSession
      .builder()
      .master("local")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .appName("spark session").getOrCreate()
  }

}
