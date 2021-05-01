package habla.doric
package types

import habla.doric.syntax.NumericOperations

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, IntegerType}

trait DoricIntegerType {

  type IntegerColumn = DoricColumn[Int]

  object IntegerColumn {

    def apply(litv: Int): IntegerColumn =
      litv.lit

    def unapply(column: Column): Option[IntegerColumn] =
      DoricColumnExtr.unapply[Int](column)
  }
  implicit val fromInt: FromDf[Int] = new FromDf[Int] {

    override def dataType: DataType = IntegerType

  }

  implicit val intArith: NumericOperations[Int] = new NumericOperations[Int] {}

  implicit val intCastToString: Casting[Int, String] =
    new SparkCasting[Int, String] {}

  implicit val intCastToLong: Casting[Int, Long] =
    new SparkCasting[Int, Long] {}

  implicit val intCastToFloat: Casting[Int, Float] =
    new SparkCasting[Int, Float] {}

  implicit val intCastToDouble: Casting[Int, Double] =
    new SparkCasting[Int, Double] {}
}
