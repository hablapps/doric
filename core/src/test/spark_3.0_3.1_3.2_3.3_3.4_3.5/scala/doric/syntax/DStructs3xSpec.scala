package doric.syntax

import scala.jdk.CollectionConverters._

import doric.{colStruct, DoricTestElements}
import java.sql.Timestamp

import org.apache.spark.sql.{functions => f}

class DStructs3xSpec extends DoricTestElements {

  import spark.implicits._

  describe("toCsv doric function") {

    val dfUsers = List(
      (
        User2("name1", "surname1", 1, Timestamp.valueOf("2015-08-26 00:00:00")),
        1
      ),
      (
        User2("name2", "surname2", 2, Timestamp.valueOf("2015-08-26 00:00:00")),
        2
      ),
      (User2("name3", "surname3", 3, null), 3)
    )
      .toDF("user", "delete")

    it("should work as to_csv spark function") {
      val expected =
        if (spark.version < "3.3.0")
          List(
            Some("name1,surname1,1,2015-08-26T00:00:00.000Z"),
            Some("name2,surname2,2,2015-08-26T00:00:00.000Z"),
            Some("name3,surname3,3,\"\"")
          )
        else
          List(
            Some("name1,surname1,1,2015-08-26T00:00:00.000Z"),
            Some("name2,surname2,2,2015-08-26T00:00:00.000Z"),
            Some("name3,surname3,3,")
          )

      dfUsers.testColumns("user")(
        c => colStruct(c).toCsv(),
        c => f.to_csv(f.col(c)),
        expected
      )
    }

    it("should work as to_csv spark function with options") {
      val expected =
        if (spark.version < "3.3.0")
          List(
            Some("name1,surname1,1,26/08/2015"),
            Some("name2,surname2,2,26/08/2015"),
            Some("name3,surname3,3,\"\"")
          )
        else
          List(
            Some("name1,surname1,1,26/08/2015"),
            Some("name2,surname2,2,26/08/2015"),
            Some("name3,surname3,3,")
          )

      dfUsers.testColumns2("user", Map("timestampFormat" -> "dd/MM/yyyy"))(
        (c, options) => colStruct(c).toCsv(options),
        (c, options) => f.to_csv(f.col(c), options.asJava),
        expected
      )
    }
  }

}
