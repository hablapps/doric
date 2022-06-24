package doric
package syntax

import doric.{DoricColumn, TypedColumnTest}
import doric.types.SparkType.Primitive
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => f}

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

protected trait NumericUtilsSpec extends TypedColumnTest {

  type FromInt[T] = Int => T
  implicit val longTrans: FromInt[Long]     = _.toLong
  implicit val doubleTrans: FromInt[Double] = _.toDouble
  implicit val floatTrans: FromInt[Float]   = _.toFloat
  type FromFloat[T] = Float => T
  implicit val floatTransD: FromFloat[Double] = _.toDouble

  def getClassName[T: ClassTag]: String = classTag[T].getClass.getSimpleName
    .replaceAll("Manifest", "")

  def getName[T: ClassTag](pos: Int = 1): String =
    s"col_${classTag[T].getClass.getSimpleName}_$pos"

  def validate[T1: Primitive: ClassTag, T2: Primitive: ClassTag, O: Primitive](
      df: DataFrame,
      f: (DoricColumn[T1], DoricColumn[T2]) => DoricColumn[O]
  ): Unit =
    df.validateColumnType(
      f(
        col[T1](getName[T1]()),
        col[T2](getName[T1]())
      )
    )

  def testDoricSpark[
      T: Primitive: ClassTag: TypeTag,
      O: Primitive: ClassTag: TypeTag
  ](
      input: List[Option[Int]],
      output: List[Option[O]],
      doricFun: DoricColumn[T] => DoricColumn[O],
      sparkFun: Column => Column
  )(implicit
      spark: SparkSession,
      funT: Int => T
  ): Unit = {
    import spark.implicits._
    val df = input.map(_.map(funT)).toDF("col1")

    df.testColumns("col1")(
      c => doricFun(col[T](c)),
      c => sparkFun(f.col(c)),
      output
    )
  }

  def testDoricSpark2[
      T1: Primitive: ClassTag: TypeTag,
      T2: Primitive: ClassTag: TypeTag,
      O: Primitive: ClassTag: TypeTag
  ](
      input: List[(Option[Int], Option[Int])],
      output: List[Option[O]],
      doricFun: (DoricColumn[T1], DoricColumn[T2]) => DoricColumn[O],
      sparkFun: (Column, Column) => Column
  )(implicit
      spark: SparkSession,
      funT1: Int => T1,
      funT2: Int => T2
  ): Unit = {
    import spark.implicits._
    val df = input
      .map { case (x, y) => (x.map(funT1), y.map(funT2)) }
      .toDF("col1", "col2")

    df.testColumns2("col1", "col2")(
      (c1, c2) => doricFun(col[T1](c1), col[T2](c2)),
      (c1, c2) => sparkFun(f.col(c1), f.col(c2)),
      output
    )
  }

  def testDoricSparkDecimals[
      T: Primitive: ClassTag: TypeTag,
      O: Primitive: ClassTag: TypeTag
  ](
      input: List[Option[Float]],
      output: List[Option[O]],
      doricFun: DoricColumn[T] => DoricColumn[O],
      sparkFun: Column => Column
  )(implicit
      spark: SparkSession,
      funT: FromFloat[T]
  ): Unit = {
    import spark.implicits._
    val df = input.map(_.map(funT)).toDF("col1")

    df.testColumns("col1")(
      c => doricFun(col[T](c)),
      c => sparkFun(f.col(c)),
      output
    )
  }

}
