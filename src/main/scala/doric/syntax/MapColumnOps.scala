package doric
package syntax

import cats.implicits._

import org.apache.spark.sql.functions.{map_keys, map_values}

trait MapColumnOps {
  implicit class MapColumnSyntax[K, V](
      private val map: DoricColumn[Map[K, V]]
  ) {

    def get(key: DoricColumn[K]): DoricColumn[V] =
      (map.elem, key.elem).mapN(_(_)).toDC

    def keys: DoricColumn[Array[K]] =
      map.elem.map(map_keys).toDC

    def values: DoricColumn[Array[V]] =
      map.elem.map(map_values).toDC

  }
}
