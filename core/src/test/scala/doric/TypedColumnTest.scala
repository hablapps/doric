package doric

import scala.reflect._
import scala.reflect.runtime.universe._

import doric.types.{Casting, SparkType}
import org.scalactic._
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.{Column, DataFrame, Encoder}
import org.apache.spark.sql.types.DataType

trait TypedColumnTest extends Matchers {

  implicit class ValidateColumnType(df: DataFrame) {

    private lazy val doricCol = "dcol".cname
    private lazy val sparkCol = "scol".cname

    /**
      * Tests doric & spark functions without parameters or columns
      *
      * @param dcolumn
      *   doric column
      * @param scolumn
      *   spark column
      * @param expected
      *   list of values
      * @tparam I1
      *   literal or column type
      * @tparam T
      *   Comparing column type
      */
    def testColumn[I1, T: SparkType: TypeTag: Equality](
        dcolumn: DoricColumn[T],
        scolumn: Column,
        expected: List[Option[T]] = List.empty
    ): Unit = {

      val result = df.select(
        dcolumn.as(doricCol),
        scolumn.asDoric[T].as(sparkCol)
      )

      compareDifferences(result, expected)
    }

    /**
      * Tests doric & spark function from a column or literal
      *
      * @param column1
      *   literal or column name
      * @param dcolumn
      *   function to get doric column
      * @param scolumn
      *   function to get spark column
      * @param expected
      *   list of values
      * @tparam I1
      *   literal or column type
      * @tparam T
      *   Comparing column type
      */
    def testColumns[I1, T: SparkType: TypeTag: Equality](
        column1: I1
    )(
        dcolumn: I1 => DoricColumn[T],
        scolumn: I1 => Column,
        expected: List[Option[T]] = List.empty
    ): Unit = {

      val result = df.select(
        dcolumn(column1).as(doricCol),
        scolumn(column1).asDoric[T].as(sparkCol)
      )

      compareDifferences(result, expected)
    }

    /**
      * Tests doric & spark function from two columns or literals
      *
      * @param column1
      *   literal or column name
      * @param column2
      *   literal or column name
      * @param dcolumn
      *   function to get doric column
      * @param scolumn
      *   function to get spark column
      * @param expected
      *   list of values
      * @tparam I1
      *   literal or column type
      * @tparam I2
      *   literal or column type
      * @tparam T
      *   Comparing column type
      */
    def testColumns2[I1, I2, T: SparkType: TypeTag: Equality](
        column1: I1,
        column2: I2
    )(
        dcolumn: (I1, I2) => DoricColumn[T],
        scolumn: (I1, I2) => Column,
        expected: List[Option[T]] = List.empty
    ): Unit = {

      val result = df.select(
        dcolumn(column1, column2).as(doricCol),
        scolumn(column1, column2).asDoric[T].as(sparkCol)
      )

      compareDifferences(result, expected)
    }

    /**
      * Tests doric & spark function from three columns or literals
      *
      * @param column1
      *   literal or column name
      * @param column2
      *   literal or column name
      * @param column3
      *   literal or column name
      * @param dcolumn
      *   function to get doric column
      * @param scolumn
      *   function to get spark column
      * @param expected
      *   list of values
      * @tparam I1
      *   literal or column type
      * @tparam I2
      *   literal or column type
      * @tparam I3
      *   literal or column type
      * @tparam T
      *   Comparing column type
      */
    def testColumns3[I1, I2, I3, T: SparkType: TypeTag: Equality](
        column1: I1,
        column2: I2,
        column3: I3
    )(
        dcolumn: (I1, I2, I3) => DoricColumn[T],
        scolumn: (I1, I2, I3) => Column,
        expected: List[Option[T]] = List.empty
    ): Unit = {

      val result = df.select(
        dcolumn(column1, column2, column3).as(doricCol),
        scolumn(column1, column2, column3).asDoric[T].as(sparkCol)
      )

      compareDifferences(result, expected)
    }

    /**
      * Tests doric & spark function from four columns or literals
      *
      * @param column1
      *   literal or column name
      * @param column2
      *   literal or column name
      * @param column3
      *   literal or column name
      * @param column4
      *   literal or column name
      * @param dcolumn
      *   function to get doric column
      * @param scolumn
      *   function to get spark column
      * @param expected
      *   list of values
      * @tparam I1
      *   literal or column type
      * @tparam I2
      *   literal or column type
      * @tparam I3
      *   literal or column type
      * @tparam I4
      *   literal or column type
      * @tparam T
      *   Comparing column type
      */
    def testColumns4[I1, I2, I3, I4, T: SparkType: TypeTag: Equality](
        column1: I1,
        column2: I2,
        column3: I3,
        column4: I4
    )(
        dcolumn: (I1, I2, I3, I4) => DoricColumn[T],
        scolumn: (I1, I2, I3, I4) => Column,
        expected: List[Option[T]] = List.empty
    ): Unit = {

      val result = df.select(
        dcolumn(column1, column2, column3, column4).as(doricCol),
        scolumn(column1, column2, column3, column4).asDoric[T].as(sparkCol)
      )

      compareDifferences(result, expected)
    }

    /**
      * @param df
      *   Spark dataFrame
      * @param expected
      *   list of values
      * @tparam T
      *   Comparing column type
      */
    private def compareDifferences[T: SparkType: TypeTag: Equality](
        df: DataFrame,
        expected: List[Option[T]]
    ): Unit = {

      val equalsColumn = "equals".cname
      val result = df
        .withColumn(
          equalsColumn,
          (
            col[T](doricCol) === col(sparkCol)
              or (
                col(doricCol).isNull
                  and col(sparkCol).isNull
              )
          ).as(equalsColumn)
        )
        .na
        .fill(Map(equalsColumn.value -> false))

      implicit val enc: Encoder[(Option[T], Option[T], Boolean)] =
        result.sparkSession.implicits
          .newProductEncoder[(Option[T], Option[T], Boolean)]
      val rows = result.as[(Option[T], Option[T], Boolean)].collect().toList

      val doricColumns   = rows.map(_._1)
      val sparkColumns   = rows.map(_._2)
      val boolResColumns = rows.map(_._3)

      assert(
        boolResColumns.reduce(_ && _),
        s"\nDoric function & Spark function return different values\n" +
          s"Doric   : $doricColumns\n" +
          s"Spark   : $sparkColumns}" +
          s"${if (expected.nonEmpty) s"\nExpected: $expected"}"
      )

      if (expected.nonEmpty) {
        import Equalities._
        assert(
          doricColumns === expected,
          s"\nDoric and Spark functions return different values than expected"
        )
      }
    }

    def validateColumnType[T: SparkType](
        column: DoricColumn[T],
        show: Boolean = false
    ): DataFrame = {
      val colName          = "result".cname
      val df2              = df.withColumn(colName, column)
      val providedDatatype = df2(colName.value).expr.dataType
      assert(
        SparkType[T].isEqual(providedDatatype),
        s"the type of the column '$column' is not ${SparkType[T].dataType} is $providedDatatype"
      )
      if (show) {
        df2.show(false)
        df2
      } else {
        df2.foreach(_ =>
          ()
        ) // force a spark execution to check if in spark runtime the job fails
        df2
      }
    }
  }

  implicit class TestColumn[T: ClassTag: SparkType](tcolumn: DoricColumn[T]) {

    type Cast[To] = Casting[T, To]

    /**
      * Checks that the actual state of the column still has the same type of
      * the representation.
      *
      * @return
      *   the provided column
      */
    def withTypeChecked: DoricColumn[T] = {
      withTypeChecked(dataType[T])
    }

    /**
      * Checks that the actual state of the column still has the same type of
      * the representation.
      *
      * @param expectedType
      *   the spark datatype expected in this moment
      * @return
      *   the provided column
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
      * @param expectedType
      *   the spark datatype expected in this moment
      * @return
      *   the provided column casted to the type if
      */
    def testCastingTo[To: Cast: SparkType: ClassTag](
        expectedType: DataType
    ): DoricColumn[To] = {
      SparkType[To].dataType
      Casting[T, To].cast(tcolumn)
      tcolumn.cast[To].withTypeChecked.withTypeChecked(expectedType)
    }
  }

}
