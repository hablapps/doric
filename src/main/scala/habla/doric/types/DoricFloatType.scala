package habla.doric
package types

import habla.doric.syntax.NumericOperations

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, FloatType}

trait DoricFloatType {

  type FloatColumn = DoricColumn[Float]

  object FloatColumn {

    def apply(litv: Float): FloatColumn =
      litv.lit

    def unapply(column: Column): Option[FloatColumn] =
      DoricColumnExtr.unapply[Float](column)
  }

  implicit val fromFloat: SparkType[Float] = new SparkType[Float] {
    override def dataType: DataType = FloatType
  }

  implicit val floatArith: NumericOperations[Float] =
    new NumericOperations[Float] {}

  implicit val floatCastToString: Casting[Float, String] =
    new SparkCasting[Float, String] {}

  implicit val floatCastToDouble: Casting[Float, Double] =
    new SparkCasting[Float, Double] {}

  implicit val floatCastToInt: UnsafeCasting[Float, Int] =
    new SparkUnsafeCasting[Float, Int] {}

  implicit val floatCastToLong: UnsafeCasting[Float, Long] =
    new SparkUnsafeCasting[Float, Long] {}

}
