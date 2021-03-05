package mrpowers.bebe
package syntax

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.spark.sql.Column

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

    def join[T: ToColumn](df2: DataFrame, column: T): DataFrame = {
      df.join(df2, column.sparkColumn)
    }

    def join[T: ToColumn](df2: DataFrame)(f: (DataFrame, DataFrame) => T): DataFrame = {
      df.join(df2, f(df, df2).sparkColumn)
    }

    def groupBy[T: ToColumn](column: DataFrame => T): RelationalGroupedDataset = {
      df.groupBy(column(df).sparkColumn)
    }

    def groupBy[T: ToColumn](columns: T*): RelationalGroupedDataset = {
      df.groupBy(columns.map(_.sparkColumn): _*)
    }

    def groupBy[T1: ToColumn, T2: ToColumn](
        column1: T1,
        column2: T2
    ): RelationalGroupedDataset = {
      df.groupBy(column1.sparkColumn)
    }

    def groupBy[T1: ToColumn, T2: ToColumn, T3: ToColumn](
        column1: T1,
        column2: T2,
        column3: T3
    ): RelationalGroupedDataset = {
      df.groupBy(column1.sparkColumn)
    }

    def groupBy[T1: ToColumn, T2: ToColumn, T3: ToColumn, T4: ToColumn](
        column1: T1,
        column2: T2,
        column3: T3,
        column4: T4
    ): RelationalGroupedDataset = {
      df.groupBy(column1.sparkColumn)
    }

    def groupByF[T: ToColumn](f: DataFrame => T): GroupedDataFrame = {
      new GroupedDataFrame(df, f(df).sparkColumn)
    }

    def groupByF[TG: ToColumn, TA: ToColumn](
        f: DataFrame => TG
    )(agg: DataFrame => TA): DataFrame = {
      df.groupBy(f(df).sparkColumn).agg(agg(df).sparkColumn)
    }

    def groupByFMulty[TG: ToColumn, TA: ToColumn](
        f: DataFrame => Seq[TG]
    )(agg: DataFrame => Seq[TA]): DataFrame = {
      val aggColumns = agg(df).map(_.sparkColumn)
      df.groupBy(f(df).map(_.sparkColumn): _*).agg(aggColumns.head, aggColumns.tail: _*)
    }

  }

  class GroupedDataFrame private[bebe] (df: DataFrame, columns: Column*) {
    def agg[T: ToColumn](aggColumn: T, aggColumns: T*): DataFrame = {
      df.groupBy(columns: _*).agg(aggColumn.sparkColumn, aggColumns.map(_.sparkColumn): _*)
    }
  }

  implicit class GroupedDataFrameSyntax(gdf: RelationalGroupedDataset) {
    def agg[T: ToColumn](column: T, columns: T*): DataFrame = {
      gdf.agg(column.sparkColumn, columns.map(_.sparkColumn): _*)
    }

    def agg[T1: ToColumn](
        column1: T1
    ): DataFrame = {
      gdf.agg(column1.sparkColumn)
    }

    def agg[T1: ToColumn, T2: ToColumn](
        column1: T1,
        column2: T2
    ): DataFrame = {
      gdf.agg(column1.sparkColumn, column2.sparkColumn)
    }

    def agg[T1: ToColumn, T2: ToColumn, T3: ToColumn](
        column1: T1,
        column2: T2,
        column3: T3
    ): DataFrame = {
      gdf.agg(column1.sparkColumn, column2.sparkColumn, column3.sparkColumn)
    }

    def agg[T1: ToColumn, T2: ToColumn, T3: ToColumn, T4: ToColumn](
        column1: T1,
        column2: T2,
        column3: T3,
        column4: T4
    ): DataFrame = {
      gdf.agg(column1.sparkColumn, column2.sparkColumn, column3.sparkColumn, column4.sparkColumn)
    }

  }

}
