package doric
package syntax

import doric.implicitConversions._
import doric.sem.{ColumnTypeError, DoricMultiError}

import org.apache.spark.sql.{Row, functions => f}
import org.apache.spark.sql.types.{IntegerType, StringType}

class MapColumns3xSpec extends DoricTestElements with MapColumns {

  describe("filter doric function") {
    import spark.implicits._

    val df = List(
      Map("k1" -> "v1", "k2" -> "v2"),
      Map.empty[String, String],
      null
    ).toDF("col1")

    it("should work as map_filter size function") {
      df.testColumns("col1")(
        c => colMapString[String](c).filter((k, _) => k.contains("1")),
        c => f.map_filter(f.col(c), (k, _) => k.contains("1")),
        List(
          Map("k1" -> "v1"),
          Map.empty[String, String],
          null
        ).map(Option(_))
      )
    }
  }

  describe("zipWith doric function") {
    import spark.implicits._

    val df = List(
      (Map("k1" -> "v11", "k2" -> "v2"), Map("k1" -> "v12", "k3" -> "v3")),
      (Map.empty[String, String], Map("k1" -> "v1", "k2" -> "v2")),
      (null, Map("k1" -> "v1", "k2" -> "v2")),
      (Map("k1" -> "v1", "k2" -> "v2"), null),
      (null, null)
    ).toDF("col1", "col2")

    it("should work as map_zip_with size function") {
      df.testColumns2("col1", "col2")(
        (c1, c2) =>
          colMapString[String](c1).zipWith[String, String](
            colMapString[String](c2),
            (_, v1, v2) => v1 + v2
          ),
        (c1, c2) =>
          f.map_zip_with(f.col(c1), f.col(c2), (_, v1, v2) => f.concat(v1, v2)),
        List(
          Map("k1" -> "v11v12", "k2" -> null, "k3" -> null),
          Map("k1" -> null, "k2"     -> null),
          null,
          null,
          null
        ).map(Option(_))
      )
    }
  }

  describe("transformKeys doric function") {
    import spark.implicits._

    val df = List(
      Map("k1" -> "v1", "k2" -> "v2"),
      Map.empty[String, String],
      null
    ).toDF("col1")

    it("should work as transform_keys size function") {
      df.testColumns2("col1", "_key")(
        (c, l) => colMapString[String](c).transformKeys((k, _) => k + l.lit),
        (c, l) => f.transform_keys(f.col(c), (k, _) => f.concat(k, f.lit(l))),
        List(
          Map("k1_key" -> "v1", "k2_key" -> "v2"),
          Map.empty[String, String],
          null
        ).map(Option(_))
      )
    }
  }

  describe("transformValues doric function") {
    import spark.implicits._

    val df = List(
      Map("k1" -> "v1", "k2" -> "v2"),
      Map.empty[String, String],
      null
    ).toDF("col1")

    it("should work as transform_values size function") {
      df.testColumns("col1")(
        c => colMapString[String](c).transformValues((k, v) => k + v),
        c => f.transform_values(f.col(c), (k, v) => f.concat(k, v)),
        List(
          Map("k1" -> "k1v1", "k2" -> "k2v2"),
          Map.empty[String, String],
          null
        ).map(Option(_))
      )

      intercept[DoricMultiError] {
        List(
          Map("k1" -> ("v1", 1), "k2" -> ("v2", 2)),
          Map.empty[String, (String, Int)],
          null
        ).toDF("col1")
          .select(
            col[Map[String, Row]]("col1").transformValues((a, b) =>
              a + b.getChild("_3") + b.getChild[Int]("_1").cast
            )
          )
      } should containAllErrors(
        ChildColumnNotFound("_3", List("_1", "_2")),
        ColumnTypeError("_1", IntegerType, StringType)
      )
    }
  }
}
