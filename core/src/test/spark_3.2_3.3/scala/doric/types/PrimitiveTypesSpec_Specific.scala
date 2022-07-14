package doric
package types

import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.CalendarInterval

trait PrimitiveTypesSpec_Specific extends DoricTestElements {

  describe("Simple Java/Scala types (since Spark 3.2)") {

    it("should match Atomic Spark SQL types") {

      // Interval type

      testDataType[java.time.Duration]
      testDataType[java.time.Period]

      /*
      testLitDataType[java.time.Duration](java.time.Duration.ZERO)
      testLitDataType[java.time.Period](java.time.Period.ZERO)
       */
    }
  }

}