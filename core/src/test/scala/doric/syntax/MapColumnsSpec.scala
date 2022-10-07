package doric
package syntax

import doric.implicitConversions._
import org.apache.spark.sql.{Row, functions => f}

import java.sql.Timestamp
import scala.collection.JavaConverters._

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

    it("can be retrieved as map column") {
      df.validateColumnType(colMap[String, Int]("col"))
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

  describe("mapEntries doric function") {
    import spark.implicits._

    val df = List(
      Map("k1" -> "v1", "k2" -> "v2"),
      Map.empty[String, String],
      null
    ).toDF("col1")

    it("should work as spark map_entries function") {
      df.testColumns("col1")(
        c => colMapString[String](c).mapEntries,
        c => f.map_entries(f.col(c)),
        List(
          Some(Array(Row("k1", "v1"), Row("k2", "v2"))),
          Some(Array.empty[Row]),
          None
        )
      )
    }
  }

  describe("explode doric function (for maps)") {
    import spark.implicits._

    it("should work as spark explode function") {
      val df = List(
        ("1", Map("a" -> "b", "c" -> "d")),
        ("2", Map.empty[String, String]),
        ("3", null)
      ).toDF("ix", "col")

      val rows = df
        .select(colString("ix"), colMapString[String]("col").explode)
        .as[(String, String, String)]
        .collect()
        .toList
      rows shouldBe df
        .select(f.col("ix"), f.explode(f.col("col")))
        .as[(String, String, String)]
        .collect()
        .toList
      rows.map(Option(_)) shouldBe List(
        Some("1", "a", "b"),
        Some("1", "c", "d")
      )
    }
  }

  describe("explodeOuter doric function (for maps)") {
    import spark.implicits._

    it("should work as spark explode_outer function") {
      val df = List(
        ("1", Map("a" -> "b", "c" -> "d")),
        ("2", Map.empty[String, String]),
        ("3", null)
      ).toDF("ix", "col")

      val rows = df
        .select(colString("ix"), colMapString[String]("col").explodeOuter)
        .as[(String, String, String)]
        .collect()
        .toList
      rows shouldBe df
        .select(f.col("ix"), f.explode_outer(f.col("col")))
        .as[(String, String, String)]
        .collect()
        .toList
      rows.map(Option(_)) shouldBe List(
        Some("1", "a", "b"),
        Some("1", "c", "d"),
        Some("2", null, null),
        Some("3", null, null)
      )
    }
  }

  describe("posExplode doric function (for maps)") {
    import spark.implicits._

    it("should work as spark posexplode function") {
      val df = List(
        ("1", Map("a" -> "b", "c" -> "d")),
        ("2", Map.empty[String, String]),
        ("3", null)
      ).toDF("ix", "col")

      val rows = df
        .select(colString("ix"), colMapString[String]("col").posExplode)
        .as[(String, Int, String, String)]
        .collect()
        .toList
      rows shouldBe df
        .select(f.col("ix"), f.posexplode(f.col("col")))
        .as[(String, Int, String, String)]
        .collect()
        .toList
      rows.map(Option(_)) shouldBe List(
        Some("1", 0, "a", "b"),
        Some("1", 1, "c", "d")
      )
    }
  }

  describe("posExplodeOuter doric function (for maps)") {
    import spark.implicits._

    it("should work as spark posexplode_outer function") {
      val df = List(
        ("1", Map("a" -> "b", "c" -> "d")),
        ("2", Map.empty[String, String]),
        ("3", null)
      ).toDF("ix", "col")

      val rows = df
        .select(colString("ix"), colMapString[String]("col").posExplodeOuter)
        .as[(String, java.lang.Integer, String, String)]
        .collect()
        .toList
      rows shouldBe df
        .select(f.col("ix"), f.posexplode_outer(f.col("col")))
        .as[(String, java.lang.Integer, String, String)]
        .collect()
        .toList
      rows.map(Option(_)) shouldBe List(
        Some("1", 0, "a", "b"),
        Some("1", 1, "c", "d"),
        Some("2", null, null, null),
        Some("3", null, null, null)
      )
    }
  }

  describe("toJson(map) doric function") {

    val dfUsers = List(
      Map(
        "user1" -> User2(
          "name1",
          "surname1",
          1,
          Timestamp.valueOf("2015-08-26 00:00:00")
        ),
        "user2" -> User2(
          "name2",
          "surname2",
          2,
          Timestamp.valueOf("2015-08-26 00:00:00")
        )
      ),
      Map("user3" -> User2("name3", "surname3", 3, null))
    )
      .toDF("user")

    it("should work as to_json spark function") {
      dfUsers.testColumns("user")(
        c => colMap[String, Row](c).toJson(),
        c => f.to_json(f.col(c)),
        List(
          Some(
            "{" +
              """"user1":{"name":"name1","surname":"surname1","age":1,"birthday":"2015-08-26T00:00:00.000Z"},""" +
              """"user2":{"name":"name2","surname":"surname2","age":2,"birthday":"2015-08-26T00:00:00.000Z"}""" +
              "}"
          ),
          Some(
            """{"user3":{"name":"name3","surname":"surname3","age":3}}"""
          )
        )
      )
    }

    it("should work as to_json spark function with options") {
      dfUsers.testColumns2("user", Map("timestampFormat" -> "dd/MM/yyyy"))(
        (c, options) => colMap[String, Row](c).toJson(options),
        (c, options) => f.to_json(f.col(c), options.asJava),
        List(
          Some(
            "{" +
              """"user1":{"name":"name1","surname":"surname1","age":1,"birthday":"26/08/2015"},""" +
              """"user2":{"name":"name2","surname":"surname2","age":2,"birthday":"26/08/2015"}""" +
              "}"
          ),
          Some(
            """{"user3":{"name":"name3","surname":"surname3","age":3}}"""
          )
        )
      )
    }
  }

}
