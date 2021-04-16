package habla.doric
package syntax

class MapColumnOpsSpec extends DoricTestElements with MapColumnOps {

  import spark.implicits._

  private val df = List((Map("hola" -> 15), "hola"))
    .toDF("col", "validkey")

  describe("Map column") {
    it("can get values from the keys") {
      df.validateColumnType(get[Map[String, Int]]("col").get(getString("validkey")))
      df.validateColumnType(get[Map[String, Int]]("col").get("validkey"))
    }

    it("can get the keys array") {
      df.validateColumnType(get[Map[String, Int]]("col").keys)
    }

    it("can get the values array") {
      df.validateColumnType(get[Map[String, Int]]("col").values)
    }
  }

}
