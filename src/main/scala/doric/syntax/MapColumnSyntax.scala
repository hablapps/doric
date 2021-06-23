package doric
package syntax

import cats.implicits._

import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.functions.{map_keys, map_values}

trait MapColumnSyntax {

  /**
    * Creates a new map column. The array in the first column is used for keys. The array in the
    * second column is used for values. All elements in the array for key should not be null.
    * @param keys the array to create the keys.
    * @param values the array to create the values.
    * @tparam K the type of the Array elements of the keys.
    * @tparam V the type of the Array elements of the value.
    * @return an DoricColumn of type Map of the keys and values.
    */
  def mapFromArrays[K, V](
      keys: DoricColumn[Array[K]],
      values: DoricColumn[Array[V]]
  ): DoricColumn[Map[K, V]] = {
    (keys.elem, values.elem).mapN((k, v) => f.map_from_arrays(k, v)).toDC
  }

  /**
    * Creates a new map column. The input is formed by tuples of key and the corresponding value.
    * @param first a pair of key value DoricColumns
    * @param rest the rest of pairs of key and corresponding Values.
    * @tparam K the type of the keys of the Map
    * @tparam V the type of the values of the Map
    * @return the DoricColumn of the corresponding Map type
    */
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

    /**
      * Returns the value if exist of the key
      * @param key the required key
      * @return a DoricColumn of the type of value, can be null if the key doesn't exist.
      */
    def get(key: DoricColumn[K]): DoricColumn[V] =
      (map.elem, key.elem).mapN(_(_)).toDC

    /**
      * @return the DoricColumn of the Array of keys
      */
    def keys: DoricColumn[Array[K]] =
      map.elem.map(map_keys).toDC

    /**
      * @return the DoricColumn of the Array of values
      */
    def values: DoricColumn[Array[V]] =
      map.elem.map(map_values).toDC

  }
}
