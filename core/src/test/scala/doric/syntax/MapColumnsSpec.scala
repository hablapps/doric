package doric
package syntax

import doric.implicitConversions._

import org.apache.spark.sql.{functions => f}

class MapColumnsSpec extends DoricTestElements with MapColumns {

  import spark.implicits._

  private val df = List((Map("hola" -> 15), "hola"))
    .toDF("col", "validkey")

  describe("Map column") {

    it("map") {
      List((1, "hola", 2, "adios"))
        .toDF("k1", "v1", "k2", "v2")
        .validateColumnType(
          map(colInt("k1") -> colString("v1"), colInt("k2") -> colString("v2"))
        )

      List((1, "hola", 2, "adios"))
        .toDF("k1", "v1", "k2", "v2")
        .validateColumnType(
          map[Int, String]((col("k1"), col("v1")), (col("k2"), col("v2")))
        )
    }

    it("mapFromArrays") {
      List((List("hola", "adios"), List(1, 2)))
        .toDF("key", "value")
        .validateColumnType(
          mapFromArrays[String, Int](colArrayString("key"), col("value"))
        )

      List((List("hola", "adios"), List(1, 2)))
        .toDF("key", "value")
        .validateColumnType(
          mapFromArrays[String, Int](col("key"), col("value"))
        )
    }

    it("can get values from the keys") {
      df.validateColumnType(
        col[Map[String, Int]]("col").get(colString("validkey"))
      )
      df.validateColumnType(col[Map[String, Int]]("col").get("validkey"))
    }

    it("can get the keys array") {
      df.validateColumnType(col[Map[String, Int]]("col").keys)
    }

    it("can get the values array") {
      df.validateColumnType(col[Map[String, Int]]("col").values)
    }
  }

  describe("concatMaps doric function") {
    import spark.implicits._

    val df = List(
      (Map("k1" -> "v1"), Map("k2" -> "v2")),
      (Map("k1" -> "v1"), null),
      (null, Map("k2" -> "v2")),
      (null, null)
    ).toDF("col1", "col2")

    it("should work as spark map_concat function") {
      // TODO failing comparison
      df.testColumns2("col1", "col2")(
        (c1, c2) =>
          concatMaps(colMap[String, String](c1), colMap[String, String](c2)),
        (c1, c2) => f.map_concat(f.col(c1), f.col(c2)),
        List(Map("k1" -> "v1", "k2" -> "v2"), null, null, null).map(Option(_))
      )
    }
  }

  describe("get doric function") {
    import spark.implicits._

    val df = List(Map("k1" -> "v1"), null).toDF("col1")

    it("should retrieve the element at the provided key") {
      df.testColumns2("col1", "k1")(
        (c, k) => colMap[String, String](c).get(k.lit),
        (c, k) => f.col(c)(k),
        List("v1", null).map(Option(_))
      )
    }
  }

  describe("key doric function") {
    import spark.implicits._

    val df = List(
      Map("k1" -> "v1", "k2" -> "v2"),
      Map.empty[String, String],
      null
    ).toDF("col1")

    it("should work as spark map_keys function") {
      df.testColumns("col1")(
        c => colMap[String, String](c).keys,
        c => f.map_keys(f.col(c)),
        List(Array("k1", "k2"), Array.empty[String], null).map(Option(_))
      )
    }
  }

  describe("values doric function") {
    import spark.implicits._

    val df = List(
      Map("k1" -> "v1", "k2" -> "v2"),
      Map.empty[String, String],
      null
    ).toDF("col1")

    it("should work as spark map_values function") {
      df.testColumns("col1")(
        c => colMap[String, String](c).values,
        c => f.map_values(f.col(c)),
        List(Array("v1", "v2"), Array.empty[String], null).map(Option(_))
      )
    }
  }

  describe("elementAt doric function") {
    import spark.implicits._

    val df = List(
      Map("k1" -> "v1", "k2" -> "v2"),
      Map.empty[String, String],
      null
    ).toDF("col1")

    it("should work as spark element_at function") {
      df.testColumns2("col1", "k2")(
        (c, p) => colMap[String, String](c).elementAt(p.lit),
        (c, p) => f.element_at(f.col(c), p),
        List("v2", null, null).map(Option(_))
      )
    }
  }

}
