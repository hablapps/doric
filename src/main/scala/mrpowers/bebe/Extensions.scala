package mrpowers.bebe

import java.sql.{Date, Timestamp}
import mrpowers.bebe.Columns._

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

    def tc: TimestampColumn = TimestampColumn(str)

    def dc: DateColumn = DateColumn(str)

    def ic: IntegerColumn = IntegerColumn(str)

  }

  implicit class DataframeMethods(df: DataFrame) {
    def get[T: FromDf](colName: String): T =
      implicitly[FromDf[T]].validate(df, colName)
  }

}
