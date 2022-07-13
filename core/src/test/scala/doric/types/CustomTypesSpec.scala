package doric
package types

import Equalities._
import doric.types.customTypes.User
import User.{userlst, userst}
import org.apache.spark.sql.types.{Decimal, StringType, StructField, StructType}

class CustomTypesSpec extends DoricTestElements {

  describe("Custom Spark Type for User") {
    it("should deserialize properly") {
      SparkType[User].dataType === StringType
      SparkType[User].transform("name#10") === User("name", 10)
    }

    it("should serialize properly") {
      LiteralSparkType[User].literalTo(User("name", 10)) shouldBe "name#10"
    }

    it("should override standard Spark type for products") {
      serializeSparkType[User](User("n1", 1))
    }
  }

  describe("Custom types within complex types") {

    it("should deserialize/serialize properly in Arrays") {

      serializeSparkType[Array[User]](Array(User("n1", 10), User("n2", 10)))
      serializeSparkType[Seq[User]](Seq(User("n1", 10), User("n2", 10)))
      serializeSparkType[List[User]](List(User("n1", 10), User("n2", 10)))
      serializeSparkType[IndexedSeq[User]](
        IndexedSeq(User("n1", 10), User("n2", 10))
      )

    }

    it("should match Spark Map types") {

      serializeSparkType[Map[Int, User]](
        Map(0 -> User("n1", 1), 1 -> User("n2", 2))
      )
      serializeSparkType[Map[User, Int]](
        Map(User("n1", 1) -> 0, User("n2", 2) -> 1)
      )
    }

    it("should match Spark Option types") {

      serializeSparkType[Option[User]](Some(User("n1", 1)))
      serializeSparkType[Option[User]](None)
    }

    ignore("should match case classes") {

      SparkType[(User, String)].dataType should ===(
        StructType(
          Array(
            StructField("_1", StringType, true),
            StructField("_2", StringType, true)
          )
        )
      )
      serializeSparkType[(User, String)]((User("n1", 1), ""))
    }

    ignore("should match a combination of Spark types") {
      serializeSparkType[Array[Array[Int]]](Array(Array(1)))
      serializeSparkType[Array[Array[User]]](
        Array(Array(User("", 0), User("", 0)), Array())
      )
      serializeSparkType[Array[List[User]]](
        Array(List(User("", 0), User("", 0)), List())
      )
      serializeSparkType[Map[Int, Option[List[User]]]](
        Map(0 -> None, 1 -> Some(List(User("", 0))))
      )
      serializeSparkType[(List[Int], User, Map[Int, Option[User]])](
        (List(0, 0), User("", 0), Map(0 -> None, 1 -> Some(User("", 0))))
      )
    }
  }

}
