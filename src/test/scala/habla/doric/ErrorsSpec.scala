package habla.doric

import org.scalatest.matchers.should.Matchers

class ErrorsSpec extends DoricTestElements with Matchers {
  describe("Single error") {
    it("should contain the location of the creation of the error") {
      val error = DoricSingleError("an error!!")
      error.location.fileName.value shouldBe "ErrorsSpec.scala"
      error.location.lineNumber.value shouldBe 8
    }
  }

}
