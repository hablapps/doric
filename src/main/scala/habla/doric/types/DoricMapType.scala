package habla.doric
package types

import org.apache.spark.sql.types.{DataType, MapType}

trait DoricMapType {

  type MapColumn[K, V] = DoricColumn[Map[K, V]]

  implicit def fromMap[K: FromDf, V: FromDf]: FromDf[Map[K, V]] =
    new FromDf[Map[K, V]] {
      override def dataType: DataType =
        MapType(FromDf[K].dataType, FromDf[V].dataType)

      override def isValid(column: DataType): Boolean = column match {
        case MapType(keyType, valueType, _) =>
          FromDf[K].isValid(keyType) && FromDf[V].isValid(valueType)
        case _ => false
      }
    }

}
