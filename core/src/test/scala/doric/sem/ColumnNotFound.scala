package doric.sem

import org.apache.spark.sql.SparkSession

object ColumnNotFound {
  def apply(expectedCol: String, foundCols: List[String])(implicit
      location: Location,
      sparkSession: SparkSession
  ): SparkErrorWrapper = {

    SparkErrorWrapper(
      new Throwable(
        if (
          !(sparkSession.version.startsWith("3.4") || sparkSession.version
            .startsWith("3.5"))
        )
          s"""Cannot resolve column name "$expectedCol" among (${foundCols
              .mkString(", ")})"""
        else
          s"[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `$expectedCol` cannot be resolved. Did you mean one of the following? [${foundCols
              .mkString("`", "`, `", "`")}]."
      )
    )
  }
}
