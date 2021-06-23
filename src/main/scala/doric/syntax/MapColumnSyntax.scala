package doric
package syntax

import cats.implicits._

import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.functions.{map_keys, map_values}

trait MapColumnSyntax {

  def mapFromArrays[K, V](
      keys: DoricColumn[Array[K]],
      values: DoricColumn[Array[V]]
  ): DoricColumn[Map[K, V]] = {
    (keys.elem, values.elem).mapN((k, v) => f.map_from_arrays(k, v)).toDC
  }

  def map[K, V](
      first: (DoricColumn[K], DoricColumn[V]),
      rest: (DoricColumn[K], DoricColumn[V])*
  ): DoricColumn[Map[K, V]] = {
    val list: List[Doric[Column]] =
      List(first._1.elem, first._2.elem) ++ rest.flatMap(x =>
        List(x._1.elem, x._2.elem)
      )
    list.sequence.map(x => f.map(x: _*)).toDC
  }

  implicit class MapColumnOps[K, V](
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
