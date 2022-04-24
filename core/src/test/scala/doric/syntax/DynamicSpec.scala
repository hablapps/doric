package doric
package syntax

import cats.data.NonEmptyChain
import doric.sem.{ChildColumnNotFound, ColumnTypeError, DoricMultiError}
import doric.types.SparkType
import org.apache.spark.sql.Row
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

class DynamicSpec extends DoricTestElements with EitherValues with Matchers {

  import spark.implicits._

  private val df = List((User("John", "doe", 34), 1))
    .toDF("col", "delete")

  describe("Dynamic accessors") {

    xx
    it("can get values from sub-columns of struct columns") {
      df.validateColumnType(colStruct("col").name[String])
      df.validateColumnType(colStruct("col").age[Int])
    }

    it("can get values from sub-sub-columns") {
      List(((("1", 2.0),2),true)).toDF.validateColumnType(colStruct("_1")._1[Row]._1[String])
    }

    it("can get values from top row") {
      df.validateColumnType(row.col[Row])
      df.validateColumnType(row.col[Row].age[Int])
      List(("1", 2, true)).toDF.validateColumnType(row._1[String])
      List((("1",2),true)).toDF.validateColumnType(row._1[Row]._2[Int])
    }


    it("should not compile if the parent column is not a row"){
      // Unfortunately, scalatest fails to detect the compilation error
      // """spark.range(1).toDF.select(col[Long]("id").name[String])""" shouldNot compile
    }
  }
}
