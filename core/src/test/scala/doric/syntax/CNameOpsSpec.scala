package doric
package syntax

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers

class CNameOpsSpec extends DoricTestElements with EitherValues with Matchers {

  describe("CName") {
    it("should be retrievable via string") {
      "column".cname shouldBe CName("column")
    }

    it("apply should get a doric column") {
      CName("myColumn").apply[String] shouldBe a[StringColumn]
    }

    it("should be concatenable with CName") {
      val column = CName("myColumn")
      column.concat(column) shouldBe CName("myColumn" * 2)
    }

    it("should be concatenable with String") {
      val column = CName("myColumn")
      column + "OtherColumn" shouldBe CName("myColumnOtherColumn")
    }

    it("should be a subfield from other column") {
      val column = CName("myColumn")
      column / column shouldBe CName("myColumn.myColumn")
    }
  }

}
