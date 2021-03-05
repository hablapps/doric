package mrpowers.bebe

import org.apache.spark.sql.types.DataType
import scala.reflect.{ClassTag, _}

trait TypedColumnTest {
  implicit class TestColumn[T: FromDf: ToColumn: ClassTag](tcolumn: T) {

    type Cast[To] = Casting[T, To]

    /**
      * Checks that the actual state of the column still has the same type of the representation.
      *
      * @return the provided column
      */
    def withTypeChecked: T = {
      val columnType: DataType   = tcolumn.sparkColumn.expr.dataType
      val expectedType: DataType = dataType[T]
      assert(
        columnType == expectedType,
        s"the column expression type is ${columnType} but the wrapper " +
          s"${classTag[T].runtimeClass.getSimpleName()} if of type $expectedType "
      )
      tcolumn
    }

    /**
      * Checks that the actual state of the column still has the same type of the representation.
      *
      * @param expectedType the spark datatype expected in this moment
      * @return the provided column
      */
    def withTypeChecked(expectedType: DataType): T = {
      val columnType: DataType = tcolumn.sparkColumn.expr.dataType
      println(s"$columnType $expectedType")
      assert(
        columnType == expectedType,
        s"the column expression type is ${columnType} but the provided expected type is $expectedType "
      )
      tcolumn
    }

    /**
      * Cast and checks that the type is correct
      *
      * @param expectedType the spark datatype expected in this moment
      * @return the provided column casted to the type if
      */
    def testCastingTo[To: Cast: FromDf: ToColumn: ClassTag](expectedType: DataType): To = {
      tcolumn.castTo[To].withTypeChecked.withTypeChecked(expectedType)
    }
  }

}
