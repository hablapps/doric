package habla.doric
package types

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, StringType}

trait DoricStringType {

  type StringColumn = DoricColumn[String]

  object StringColumn {

    def apply(litv: String): StringColumn =
      litv.lit

    def unapply(column: Column): Option[StringColumn] =
      DoricColumnExtr.unapply[String](column)
  }

  implicit val fromStringDf: SparkType[String] = new SparkType[String] {

    override def dataType: DataType = StringType
  }

  implicit val stringCastToInt: UnsafeCasting[String, Int] =
    new SparkUnsafeCasting[String, Int] {}

  implicit val stringCastToLong: UnsafeCasting[String, Long] =
    new SparkUnsafeCasting[String, Long] {}

}
