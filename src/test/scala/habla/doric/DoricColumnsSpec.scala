package habla.doric

import java.sql.{Date, Timestamp}
import org.scalatest.EitherValues

import org.apache.spark.sql.DataFrame

class DoricColumnsSpec extends DoricTestElements with EitherValues {

  // scalafix:ok
  import spark.implicits._

  Seq((1, 2), (3, 4)).toDF("c1", "c2")

  def testValue[T: FromDf](df: DataFrame): Unit = {
    get[T]("column").elem.run(df).toEither.value
  }

  describe("each column should represent their datatype") {
    it("works for String") {
      testValue[String](List("hola").toDF("column"))
      testValue[String](List(Some("hola"), None).toDF("column"))
    }
    it("works for Int") {
      testValue[Int](List(14).toDF("column"))
      testValue[Int](List(Some(54), None).toDF("column"))
    }
    it("works for Long") {
      testValue[Long](List(14L).toDF("column"))
      testValue[Long](List(Some(54L), None).toDF("column"))
    }
    it("works for Float") {
      testValue[Float](List(14f).toDF("column"))
      testValue[Float](List(Some(54f), None).toDF("column"))
    }
    it("works for Array") {
      testValue[Array[Int]](List(List(14)).toDF("column"))
      testValue[Array[Int]](List(Some(List(54)), None).toDF("column"))
      testValue[Array[Long]](List(List(14L)).toDF("column"))
      testValue[Array[Long]](List(Some(List(54L)), None).toDF("column"))
    }
    it("works for Date") {
      testValue[Date](List(Date.valueOf("2020-01-01")).toDF("column"))
      testValue[Date](List(Some(Date.valueOf("2020-01-01")), None).toDF("column"))
    }
    val timestamp = Timestamp.valueOf("2020-01-01 01:01:901")
    it("works for Timestamp") {
      testValue[Timestamp](List(timestamp).toDF("column"))
      testValue[Timestamp](List(Some(timestamp), None).toDF("column"))
    }

    it("works for Map") {
      testValue[Map[Timestamp, Int]](List(Map(timestamp -> 10)).toDF("column"))
      testValue[Map[Timestamp, Int]](List(Some(Map(timestamp -> 10)), None).toDF("column"))
    }
  }

}
