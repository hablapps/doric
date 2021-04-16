package habla.doric
package functions

import scala.Predef.{any2stringadd => _}

import com.github.mrpowers.spark.fast.tests.ColumnComparer
import org.scalatest.funspec.AnyFunSpecLike

class WhenBuilderSpec extends AnyFunSpecLike with SparkSessionTestWrapper with ColumnComparer {

  override def convertToEqualizer[T](left: T): Equalizer[T] = new Equalizer(left)

  // scalafix:ok
  import spark.implicits._

  describe("when builder") {
    it("works like a normal spark when") {

      val df = List((100, 1), (8, 1008), (2, 3))
        .toDF("c1", "whenExpected")
        .withColumn(
          "whenResult",
          WhenBuilder[Int]()
            .caseW(getInt("c1") > 10, 1)
            .caseW(getInt("c1") > 5, getInt("c1") + 1000)
            .otherwise(3)
        )

      assertColEquality(df, "whenResult", "whenExpected")
    }

    it("puts null otherwiseNull is selected in rest of cases") {
      val df = List((100, Some(1)), (8, None), (2, None))
        .toDF("c1", "whenExpected")
        .withColumn(
          "whenResult",
          WhenBuilder[Int]()
            .caseW(getInt("c1") === 100, 1)
            .otherwiseNull
        )

      getInt("c1") === 100
      assertColEquality(df, "whenResult", "whenExpected")
    }
  }

}
