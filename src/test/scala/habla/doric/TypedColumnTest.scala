package habla.doric

import scala.reflect.{ClassTag, _}

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.DataFrame

trait TypedColumnTest {

  implicit class ValidateColumnType(df: DataFrame) {
    def validateColumnType[T: FromDf](
        column: DoricColumn[T],
        show: Boolean = false
    ): Unit = {
      val colName          = "result"
      val df2              = df.withColumn(colName, column)
      val providedDatatype = df2(colName).expr.dataType
      assert(
        FromDf[T].isValid(providedDatatype),
        s"the type of the column '$column' is not ${FromDf[T].dataType} is $providedDatatype"
      )
      if (show) {
        df2.show(false)
      } else {
        df2.foreach(_ =>
          ()
        ) //force a spark execution to check if in spark runtime the job fails
      }
    }
  }

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
      tcolumn.elem
        .map(c => {
          val columnType: DataType = c.expr.dataType
          assert(
            columnType == expectedType,
            s"the column expression type is $columnType but the wrapper " +
              s"${classTag[T].runtimeClass.getSimpleName} if of type $expectedType "
          )
          c
        })
        .toDC
    }

    /**
      * Cast and checks that the type is correct
      *
      * @param expectedType the spark datatype expected in this moment
      * @return the provided column casted to the type if
      */
    def testCastingTo[To: Cast: FromDf: ClassTag](
        expectedType: DataType
    ): DoricColumn[To] = {
      FromDf[To].dataType
      Casting[T, To].cast(tcolumn)
      tcolumn.cast[To].withTypeChecked.withTypeChecked(expectedType)
    }
  }

}
