package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{functions => f}

class CommonColumnsSpec
    extends DoricTestElements
    with EitherValues
    with Matchers {

  import doric.implicitConversions.stringCname

  describe("coalesce doric function") {
    import spark.implicits._

    it("should work as spark coalesce function with strings") {
      val df = List(("1", "1"), (null, "2"), ("3", null), (null, null))
        .toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (col1, col2) => coalesce(colString(col1), colString(col2)),
        (col1, col2) => f.coalesce(f.col(col1), f.col(col2)),
        List("1", "2", "3", null).map(Option(_))
      )
    }

    it("should work as spark coalesce function with integers") {
      val df =
        List((Some(1), Some(1)), (None, Some(2)), (Some(3), null), (null, null))
          .toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (col1, col2) => coalesce(colInt(col1), colInt(col2)),
        (col1, col2) => f.coalesce(f.col(col1), f.col(col2)),
        List(Some(1), Some(2), Some(3), None)
      )
    }
  }

  describe("hash doric function") {
    import spark.implicits._

    it("should work as spark hash function") {
      val df = List(
        ("this is a string", "123"),
        (null, "123"),
        ("123", null),
        (null, null)
      ).toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (col1, col2) => hash(colString(col1), colString(col2)),
        (col1, col2) => f.hash(f.col(col1), f.col(col2)),
        List(Some(-665298568), Some(1218575173), Some(1218575173), Some(42))
      )
    }

    it("should work with multiple types") {
      val df = List(
        ("whatever", 123, 12L, 4.0, true),
        (null, -4, -4572L, 0.0, false)
      ).toDF("string", "int", "long", "double", "boolean")

      df.testColumnsN(df.schema)(
        seq => hash(seq: _*),
        seq => f.hash(seq: _*),
        List(Some(-514390594), Some(-1274285264))
      )
    }
  }

  describe("xxhash64 doric function") {
    import spark.implicits._

    it("should work as spark xxhash64 function") {
      val df = List(
        ("this is a string", "123"),
        (null, "123"),
        ("123", null),
        (null, null)
      ).toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (col1, col2) => xxhash64(colString(col1), colString(col2)),
        (col1, col2) => f.xxhash64(f.col(col1), f.col(col2)),
        List(
          Some(-6297204973024389939L),
          Some(3994740064877260556L),
          Some(3994740064877260556L),
          Some(42L)
        )
      )
    }

    it("should work with multiple types") {
      val df = List(
        ("whatever", 123, 12L, 4.0, true),
        (null, -4, -4572L, 0.0, false)
      ).toDF("string", "int", "long", "double", "boolean")

      df.testColumnsN(df.schema)(
        seq => xxhash64(seq: _*),
        seq => f.xxhash64(seq: _*),
        List(Some(-7858579933223513963L), Some(-1356518162039135835L))
      )
    }
  }

  describe("All columns") {
    import spark.implicits._

    it("should be comparable as equals") {
      val df =
        List(("1", "1"), ("2", "1"), (null, "2"), ("3", null), (null, null))
          .toDF("col1", "col2")

      val res = df
        .select(colString("col1") === col("col2"))
        .as[Option[Boolean]]
        .collect()
        .toList

      res shouldBe List(Some(true), Some(false), None, None, None)
    }

    it("should be comparable as different") {
      val df =
        List(("1", "1"), ("2", "1"), (null, "2"), ("3", null), (null, null))
          .toDF("col1", "col2")

      val res = df
        .select(colString("col1") =!= col("col2"))
        .as[Option[Boolean]]
        .collect()
        .toList

      res shouldBe List(Some(false), Some(true), None, None, None)
    }

    it("should be transformable") {
      val df = List("is", "", null)
        .toDF("col1")

      val res = df
        .select(colString("col1").pipe(_ + "Piped".lit))
        .as[String]
        .collect()
        .toList

      res shouldBe List("isPiped", "Piped", null)
    }

    it("should comparable in a list") {
      val df = List("1", "a", null)
        .toDF("col1")

      val res = df
        .select(colString("col1").isIn("a", "b", "c"))
        .as[Option[Boolean]]
        .collect()
        .toList

      res shouldBe List(Some(false), Some(true), None)
    }

    it("should comparable as null") {
      val df = List("1", "a", null)
        .toDF("col1")

      val res = df
        .select(colString("col1").isNull)
        .as[Option[Boolean]]
        .collect()
        .toList

      res shouldBe List(Some(false), Some(false), Some(true))
    }

    it("should comparable as notNull") {
      val df = List("1", "a", null)
        .toDF("col1")

      val res = df
        .select(colString("col1").isNotNull)
        .as[Option[Boolean]]
        .collect()
        .toList

      res shouldBe List(Some(true), Some(true), Some(false))
    }
  }

  describe("least doric function") {
    import spark.implicits._

    it("should work as spark least function") {
      val df = List(
        ("this is a string", "123"),
        (null, "123"),
        ("this is a string", null),
        (null, null)
      ).toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (col1, col2) => least(colString(col1), colString(col2)),
        (col1, col2) => f.least(f.col(col1), f.col(col2)),
        List(
          Some("123"),
          Some("123"),
          Some("this is a string"),
          None
        )
      )
    }
  }

  describe("greatest doric function") {
    import spark.implicits._

    it("should work as spark greatest function") {
      val df = List(
        ("this is a string", "123"),
        (null, "123"),
        ("this is a string", null),
        (null, null)
      ).toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (col1, col2) => greatest(colString(col1), colString(col2)),
        (col1, col2) => f.greatest(f.col(col1), f.col(col2)),
        List(
          Some("this is a string"),
          Some("123"),
          Some("this is a string"),
          None
        )
      )
    }
  }

}
