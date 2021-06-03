package habla.doric
package types

import habla.doric.syntax.NumericOperations

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, DoubleType}

trait DoricDoubleType {

  type DoubleColumn = DoricColumn[Double]

  object DoubleColumn {

    def apply(litv: Double): DoubleColumn =
      litv.lit

    def unapply(column: Column): Option[DoubleColumn] =
      DoricColumnExtr.unapply[Double](column)

  }

  implicit val fromDouble: SparkType[Double] = new SparkType[Double] {

    override def dataType: DataType = DoubleType
  }

  implicit val doubleArith: NumericOperations[Double] =
    new NumericOperations[Double] {}

  implicit val doubleCastToString: Casting[Double, String] =
    new SparkCasting[Double, String] {}

  implicit val doubleCastToInt: UnsafeCasting[Double, Int] =
    new SparkUnsafeCasting[Double, Int] {}

  implicit val doubleCastToLong: UnsafeCasting[Double, Long] =
    new SparkUnsafeCasting[Double, Long] {}

  implicit val doubleCastToFloat: UnsafeCasting[Double, Float] =
    new SparkUnsafeCasting[Double, Float] {}

}
