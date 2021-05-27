package habla.doric
package types

import habla.doric.syntax.BooleanOperations

import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.sql.Column

trait DoricBooleanType {

  type BooleanColumn = DoricColumn[Boolean]

  object BooleanColumn {

    def apply(litv: Boolean): BooleanColumn =
      litv.lit

    def unapply(column: Column): Option[BooleanColumn] =
      DoricColumnExtr.unapply[Boolean](column)
  }

  implicit val fromBoolean: SparkType[Boolean] = new SparkType[Boolean] {
    override def dataType: DataType = BooleanType
  }

  implicit val booleanOps: BooleanOperations[Boolean] =
    new BooleanOperations[Boolean] {}

}
