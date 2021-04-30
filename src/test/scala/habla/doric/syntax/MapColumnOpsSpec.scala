package habla.doric
package syntax

import habla.doric.implicitConversions._

class MapColumnOpsSpec extends DoricTestElements with MapColumnOps {

  import spark.implicits._

  private val df = List((Map("hola" -> 15), "hola"))
    .toDF("col", "validkey")

  describe("Map column") {
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

}
