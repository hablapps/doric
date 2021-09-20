package org.apache.spark.sql.doric

import org.apache.spark.sql.{Column, DataFrame, Dataset}

object DataFrameExtras {

  def withColumnsE[T](
      df: Dataset[T],
      colNames: Seq[String],
      cols: Seq[Column]
  ): DataFrame =
    df.withColumns(colNames, cols)
}
