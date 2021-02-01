package mrpowers.faisca

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, typedLit, col}
import java.sql.Date
import java.sql.Timestamp

object extensions {

  implicit class StringMethods(str: String) {

    def c: Column = col(str)

    def l: Column = lit(str)

    def tl: Column = typedLit(str)

    def d: Date = Date.valueOf(str)

    def t: Timestamp = Timestamp.valueOf(str)

    def tc: TimestampColumn = TimestampColumn(str)

    def ic: IntegerColumn = IntegerColumn(str)

  }

}
