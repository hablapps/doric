package habla.doric
package functions

import org.scalatest.funspec.AnyFunSpecLike
import habla.doric.SparkSessionTestWrapper
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import habla.doric.IntegerColumn
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import Predef.{any2stringadd => _, _} // scalafix:ok
import com.fasterxml.jackson.module.scala.deser.overrides

class WhenBuilderSpec extends AnyFunSpecLike with SparkSessionTestWrapper with ColumnComparer {

  override def convertToEqualizer[T](left: T): Equalizer[T] = new Equalizer(left)

  // scalafix:ok
  import spark.implicits._

  describe("when builder") {
    it("works like a normal spark when") {

      val df = List((100, 1), (8, 1008), (2, 3))
        .toDF("c1", "whenExpected")
        .withColumn("whenResult")(df => {
          WhenBuilder[Int]()
            .caseW(df.get[Int]("c1") > 10, 1)
            .caseW(df.get[Int]("c1") > 5, df.get[Int]("c1") + 1000)
            .otherwise(3)
        })

      assertColEquality(df, "whenResult", "whenExpected")
    }

    it("puts null otherwiseNull is selected in rest of cases") {
      val df = List((100, Some(1)), (8, None), (2, None))
        .toDF("c1", "whenExpected")
        .withColumn("whenResult")(df => {
          WhenBuilder[Int]()
            .caseW(df.get[Int]("c1") === 100, 1)
            .otherwiseNull
        })

      df.get[Int]("c1") === 100
      assertColEquality(df, "whenResult", "whenExpected")
    }
  }

}
