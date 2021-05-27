package habla.doric
package types

import org.apache.spark.sql.types.{DataType, MapType}

trait DoricMapType {

  type MapColumn[K, V] = DoricColumn[Map[K, V]]

  implicit def fromMap[K: SparkType, V: SparkType]: SparkType[Map[K, V]] =
    new SparkType[Map[K, V]] {
      override def dataType: DataType =
        MapType(SparkType[K].dataType, SparkType[V].dataType)

      override def isValid(column: DataType): Boolean = column match {
        case MapType(keyType, valueType, _) =>
          SparkType[K].isValid(keyType) && SparkType[V].isValid(valueType)
        case _ => false
      }
    }

}
