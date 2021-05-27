package habla.doric
package types

import org.apache.spark.sql.types.{ArrayType, DataType}

trait DoricArrayType {

  type ArrayColumn[A] = DoricColumn[Array[A]]

  object ArrayColumn {
    def apply[A](litv: Array[A]): ArrayColumn[A] =
      litv.lit
  }

  implicit def fromArray[A: SparkType]: SparkType[Array[A]] = new SparkType[Array[A]] {
    override def dataType: DataType = ArrayType(implicitly[SparkType[A]].dataType)

    override def isValid(column: DataType): Boolean = column match {
      case ArrayType(left, _) => SparkType[A].isValid(left)
      case _                  => false
    }
  }

}
