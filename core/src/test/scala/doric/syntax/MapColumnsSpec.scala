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
      df.testColumns2("col1", "col2")(
        (c1, c2) =>
          concatMaps(colMapString[String](c1), colMapString[String](c2)),
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
        (c, k) => colMapString[String](c).get(k.lit),
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
        c => colMapString[String](c).keys,
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
        c => colMapString[String](c).values,
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
        (c, p) => colMapString[String](c).elementAt(p.lit),
        (c, p) => f.element_at(f.col(c), p),
        List("v2", null, null).map(Option(_))
      )
    }
  }

  // TODO explode function tests

  describe("size doric function") {
    import spark.implicits._

    val df = List(
      Map("k1" -> "v1", "k2" -> "v2"),
      Map.empty[String, String],
      null
    ).toDF("col1")

    it("should work as spark size function") {
      df.testColumns("col1")(
        c => colMapString[String](c).size,
        c => f.size(f.col(c)),
        List(Some(2), Some(0), Some(-1))
      )
    }
  }

  // TODO mapEntries

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
    }
  }

}
