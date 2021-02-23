package mrpowers.bebe

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, typedLit}

object Extensions {

  implicit class IntMethods(int: Int) {

    def l: Column = lit(int)

  }

  implicit class StringMethods(str: String) {

    def c: Column = col(str)

    def l: Column = lit(str)

    def tl: Column = typedLit(str)

    def d: Date = Date.valueOf(str)

    def t: Timestamp = Timestamp.valueOf(str)

  }

}
