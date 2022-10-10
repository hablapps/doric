package doric
package syntax

class LiteralConversionsSpec extends DoricTestElements {

  describe("Primitive doric columns") {

    it("should extract the original value of a literal(str)") {
      val literal = "myValue"
      // This casting is necessary, otherwise it would be a LiteralDoricColumn and we could access to its value with another method
      val litColumn: StringColumn = literal.lit

      litColumn.getValueIfLiteral shouldBe Some(literal)
    }

    it("should extract the original value of a literal(int)") {
      val literal = 123
      // This casting is necessary, otherwise it would be a LiteralDoricColumn and we could access to its value with another method
      val litColumn: IntegerColumn = literal.lit

      litColumn.getValueIfLiteral shouldBe Some(literal)
    }

    it("should avoid an error if it is not a literal") {
      val colStr = colString("myColumn")

      colStr.getValueIfLiteral shouldBe None
    }
  }

}
