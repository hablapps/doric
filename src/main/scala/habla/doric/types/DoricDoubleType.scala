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

  implicit val doubleArith: NumericOperations[DoubleColumn] =
    new NumericOperations[DoubleColumn] {}

  implicit val doubleCastToString: Casting[DoubleColumn, StringColumn] =
    new SparkCasting[DoubleColumn, StringColumn] {}

}
