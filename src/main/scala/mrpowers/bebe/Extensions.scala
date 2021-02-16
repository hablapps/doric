package mrpowers.bebe

import java.sql.{Date, Timestamp}
import mrpowers.bebe.Columns.{ToColumn, _}

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, typedLit}

object Extensions {

  implicit class IntMethods(int: Int) {

    def il: IntegerColumn = IntegerColumn(int)

    def l: Column = lit(int)

  }

  implicit class StringMethods(str: String) {

    def c: Column = col(str)

    def l: Column = lit(str)

    def tl: Column = typedLit(str)

    def d: Date = Date.valueOf(str)

    def t: Timestamp = Timestamp.valueOf(str)

  }

  implicit class DataframeMethods(df: DataFrame) {
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
      * for instance, via loops in order to add multiple columns can generate big plans which
      * can cause performance issues and even `StackOverflowException`.
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
      * for instance, via loops in order to add multiple columns can generate big plans which
      * can cause performance issues and even `StackOverflowException`.
      */
    def withColumn[T: ToColumn](colName: String)(col: DataFrame => T): DataFrame =
      df.withColumn(colName, implicitly[ToColumn[T]].column(col(df)))

  }

  implicit class BasicCol[T: FromDf: ToColumn](column: T){
    @inline def from: FromDf[T] = implicitly
    @inline def to: ToColumn[T] = implicitly

    def as(colName: String): T = from.construct(to.column(column) as colName)

  }

}
