package doric.sem

import org.apache.spark.sql.SparkSession

object ChildColumnNotFound {
  def apply(expectedCol: String, foundCols: List[String])(implicit
      location: Location,
      sparkSession: SparkSession
  ): SparkErrorWrapper = {
    SparkErrorWrapper(
      new Throwable(
        if (!sparkSession.version.startsWith("3.4"))
          s"No such struct field $expectedCol in ${foundCols.mkString(", ")}"
        else
          s"[FIELD_NOT_FOUND] No such struct field `$expectedCol` in ${foundCols
              .mkString("`", "`, `", "`")}."
      )
    )
  }
}
