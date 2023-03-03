package doric
package control

import scala.Predef.{any2stringadd => _}
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import doric.implicitConversions._
import doric.types.SparkType
import org.scalatest.funspec.AnyFunSpecLike
import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers

class WhenBuilderSpec
    extends AnyFunSpecLike
    with SparkSessionTestWrapper
    with ColumnComparer
    with Matchers {

  override def convertToEqualizer[T](left: T): Equalizer[T] = new Equalizer(
    left
  )

  // scalafix:ok
  import spark.implicits._

  describe("when builder") {
    val whenResult = "whenResult"
    it("works like a normal spark when") {

      val df = List((100, 1), (8, 1008), (2, 3))
        .toDF("c1", "whenExpected")
        .withColumn(
          whenResult,
          when[Int]
            .caseW(colInt("c1") > 10, 1)
            .caseW(colInt("c1") > 5, colInt("c1") + 1000)
            .otherwise(3)
        )

      assertColEquality(df, whenResult, "whenExpected")
    }

    it("puts null otherwiseNull is selected in rest of cases") {
      val whenStructure: DoricColumn[Int] = when[Int]
        .caseW(colInt("c1") === lit(100), 1)
        .otherwiseNull
      val df = List((100, Some(1)), (8, None), (2, None))
        .toDF("c1", "whenExpected")
        .withColumn(
          whenResult,
          whenStructure
        )

      colInt("c1") === 100
      assertColEquality(df, whenResult, "whenExpected")
    }

    it("case that only returns otherwise null") {
      val df = spark.range(3)

      def nullOfType[T: SparkType] = {
        val whenT: DoricColumn[T] = when[T].otherwiseNull

        val whenResultColName = whenResult
        df.withColumn(whenResultColName, whenT)
          .select(col[T](whenResultColName))
      }

      nullOfType[Int]
      nullOfType[Double]
      nullOfType[Row]
      nullOfType[Array[Int]]
    }
  }

  describe("match builder") {
    import spark.implicits._
    val matchResult = "matchResult"

    it("won't let an otherwise be set if no cases") {
      "colInt(\"c1\").matches[String].otherwiseNull" shouldNot compile
    }

    it("transform a column given an equality or a function (otherwiseNull)") {

      val myCol = colInt("c1")
        .matches[String]
        .caseW(_ > lit(10), "MAX".lit)
        .caseW(lit(8), "Eight".lit)
        .otherwiseNull

      val df = List((100, "MAX"), (8, "Eight"), (2, null))
        .toDF("c1", "whenExpected")
        .withColumn(matchResult, myCol)

      assertColEquality(df, matchResult, "whenExpected")
    }

    it("transform a column given an equality or a function (otherwise)") {

      val myCol = colInt("c1")
        .matches[String]
        .caseW(lit(8), "Eight".lit)
        .caseW(_ > lit(10), "MAX".lit)
        .otherwise("-")

      val df = List((100, "MAX"), (8, "Eight"), (2, "-"))
        .toDF("c1", "whenExpected")
        .withColumn(matchResult, myCol)

      assertColEquality(df, matchResult, "whenExpected")
    }
  }

}
