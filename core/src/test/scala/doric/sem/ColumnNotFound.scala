package doric.sem

import org.apache.spark.sql.SparkSession

object ColumnNotFound {
  def apply(
      expectedCol: String,
      foundCols: List[String],
      oldFormat: Boolean = true
  )(implicit
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
