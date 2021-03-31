package habla.doric

import com.github.mrpowers.spark.fast.tests.ColumnComparer
import habla.doric.Extensions._
import org.scalatest.funspec.AnyFunSpecLike

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class ColumnExtractor extends AnyFunSpecLike with SparkSessionTestWrapper with ColumnComparer {

  import spark.implicits._

  describe("column_extractor") {

    def transformDateOrTimestamp(colName: String)(df: DataFrame): IntegerColumn =
      df(colName) match {
        case DateColumn(dc)      => dc.dayOfMonth
        case TimestampColumn(tc) => tc.dayOfMonth
        case _                   => 0.lit
      }

    it("extracts according to the column") {
      val df = Seq(
        ("2020-01-05".d, 5, 0),
        ("2019-04-13".d, 13, 0)
      ).toDF("date", "date_expected_month", "integer_expected_month")
        .withColumn("date_month")(transformDateOrTimestamp("date"))
        .withColumn("integer_month")(transformDateOrTimestamp("date_expected_month"))

      assertColumnEquality(df, "date_month", "date_expected_month")
      assertColumnEquality(df, "integer_month", "integer_expected_month")
    }

  }

}
