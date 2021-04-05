package habla.doric

import org.apache.spark.sql.types.DataType
import scala.reflect.{ClassTag, _}

trait TypedColumnTest {
  implicit class TestColumn[T: ClassTag](tcolumn: DoricColumn[T]) {

    type Cast[To] = Casting[T, To]

    /**
      * Checks that the actual state of the column still has the same type of the representation.
      *
      * @return the provided column
      */
    def withTypeChecked(implicit fromdf: FromDf[T]): DoricColumn[T] = {
      withTypeChecked(dataType[T])
    }

    /**
      * Checks that the actual state of the column still has the same type of the representation.
      *
      * @param expectedType the spark datatype expected in this moment
      * @return the provided column
      */
    def withTypeChecked(expectedType: DataType): DoricColumn[T] = {
      tcolumn.map(c => {
        val columnType: DataType = c.expr.dataType
        assert(
          columnType == expectedType,
          s"the column expression type is ${columnType} but the wrapper " +
            s"${classTag[T].runtimeClass.getSimpleName()} if of type $expectedType "
        )
        c
      })
    }

    /**
      * Cast and checks that the type is correct
      *
      * @param expectedType the spark datatype expected in this moment
      * @return the provided column casted to the type if
      */
    def testCastingTo[To: Cast: FromDf: ClassTag](expectedType: DataType): DoricColumn[To] = {
      FromDf[To].dataType
      Casting[T, To].cast(tcolumn)
      tcolumn.castTo[To].withTypeChecked.withTypeChecked(expectedType)
    }
  }

}
