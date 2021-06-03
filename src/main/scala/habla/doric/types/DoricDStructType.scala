package habla.doric
package types

import org.apache.spark.sql.types.{DataType, StructType}

trait DoricDStructType {

  type DStructColumn = DoricColumn[DStruct]

  implicit val fromDStruct: SparkType[DStruct] = new SparkType[DStruct] {
    override def dataType: DataType = StructType(Seq.empty)

    override def isValid(column: DataType): Boolean = column match {
      case StructType(_) => true
      case _             => false
    }
  }

}
