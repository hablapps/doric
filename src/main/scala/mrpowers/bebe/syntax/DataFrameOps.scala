package mrpowers.bebe
package syntax

import org.apache.spark.sql.DataFrame

trait DataFrameOps {

  implicit class DataframeSyntax(df: DataFrame) {
    def get[T: FromDf](colName: String): T =
      implicitly[FromDf[T]].validate(df, colName)

    /**
      * Returns a new Dataset by adding a column or replacing the existing column that has
      * the same name.
      *
      * `column`'s expression must only refer to attributes supplied by this Dataset. It is an
      * error to add a column that refers to some other Dataset.
      *
      * @note this method introduces a projection internally. Therefore, calling it multiple times,
      *       for instance, via loops in order to add multiple columns can generate big plans which
      *       can cause performance issues and even `StackOverflowException`.
      */
    def withColumn[T: ToColumn](colName: String, col: T): DataFrame =
      df.withColumn(colName, implicitly[ToColumn[T]].column(col))

    /**
      * Returns a new Dataset by adding a column or replacing the existing column that has
      * the same name.
      *
      * The col function will provide the called dataframe
      *
      * @note this method introduces a projection internally. Therefore, calling it multiple times,
      *       for instance, via loops in order to add multiple columns can generate big plans which
      *       can cause performance issues and even `StackOverflowException`.
      */
    def withColumn[T: ToColumn](colName: String)(col: DataFrame => T): DataFrame =
      df.withColumn(colName, implicitly[ToColumn[T]].column(col(df)))

  }

}
