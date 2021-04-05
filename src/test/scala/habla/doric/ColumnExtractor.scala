package habla.doric

import cats.data.ValidatedNec
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import habla.doric.Extensions._
import org.scalatest.funspec.AnyFunSpecLike

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}

class ColumnExtractor extends AnyFunSpecLike with SparkSessionTestWrapper with ColumnComparer {

  import spark.implicits._

  describe("column_extractor") {

    def transformDateOrTimestamp(
        colName: String
    )(df: DataFrame): ValidatedNec[Throwable, Column] = {
      val f = df(colName) match {
        case DateColumn(dc)      => dc.dayOfMonth
        case TimestampColumn(tc) => tc.dayOfMonth
        case _                   => 0.lit[Int]
      }
      f.col(df)
    }

    it("extracts according to the column") {
      val df = Seq(
        ("2020-01-05".d, 5, 0),
        ("2019-04-13".d, 13, 0)
      ).toDF("date", "date_expected_month", "integer_expected_month")
        .withColumn("date_month", DoricColumn[Int](transformDateOrTimestamp("date")))
        .withColumn(
          "integer_month",
          DoricColumn[Int](transformDateOrTimestamp("date_expected_month"))
        )

      assertColumnEquality(df, "date_month", "date_expected_month")
      assertColumnEquality(df, "integer_month", "integer_expected_month")
    }

  }

}
