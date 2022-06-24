package doric.sem

import org.apache.spark.sql.SparkSession

object ColumnNotFound {
  def apply(expectedCol: String, foundCols: List[String])(implicit
      location: Location,
      sparkSession: SparkSession
  ): SparkErrorWrapper = {

    SparkErrorWrapper(
      new Throwable(
        s"""Cannot resolve column name "$expectedCol" among (${foundCols
            .mkString(", ")})"""
      )
    )
  }
}
