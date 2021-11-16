package doric
package syntax

import cats.implicits._
import org.apache.spark.sql.catalyst.expressions.{ElementAt, MapFilter, MapZipWith, TransformKeys, TransformValues}
import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.functions.{map_keys, map_values}

private[syntax] trait MapColumns {

  /**
    * Returns the union of all the given maps.
    *
    * @group Map Type
    * @see [[org.apache.spark.sql.functions.map_concat]]
    */
  def concatMaps[K, V](
      col: MapColumn[K, V],
      cols: MapColumn[K, V]*
  ): MapColumn[K, V] =
    (col +: cols).toList.traverse(_.elem).map(f.map_concat(_: _*)).toDC

  /**
    * Creates a new map column. The array in the first column is used for keys.
    * The array in the second column is used for values. All elements in the
    * array for key should not be null.
    *
    * @group Map Type
    * @param keys
    *   the array to create the keys.
    * @param values
    *   the array to create the values.
    * @tparam K
    *   the type of the Array elements of the keys.
    * @tparam V
    *   the type of the Array elements of the value.
    * @return
    *   an DoricColumn of type Map of the keys and values.
    * @see [[org.apache.spark.sql.functions.map_from_arrays]]
    */
  def mapFromArrays[K, V](
      keys: DoricColumn[Array[K]],
      values: DoricColumn[Array[V]]
  ): MapColumn[K, V] = {
    (keys.elem, values.elem).mapN((k, v) => f.map_from_arrays(k, v)).toDC
  }

  /**
    * Creates a new map column. The input is formed by tuples of key and the
    * corresponding value.
    *
    * @group Map Type
    * @param first
    *   a pair of key value DoricColumns
    * @param rest
    *   the rest of pairs of key and corresponding Values.
    * @tparam K
    *   the type of the keys of the Map
    * @tparam V
    *   the type of the values of the Map
    * @return
    *   the DoricColumn of the corresponding Map type
    * @see [[org.apache.spark.sql.functions.map]]
    */
  def map[K, V](
      first: (DoricColumn[K], DoricColumn[V]),
      rest: (DoricColumn[K], DoricColumn[V])*
  ): MapColumn[K, V] = {
    val list: List[Doric[Column]] =
      List(first._1.elem, first._2.elem) ++ rest.flatMap(x =>
        List(x._1.elem, x._2.elem)
      )
    list.sequence.map(x => f.map(x: _*)).toDC
  }

  /**
    * Extension methods for Map Columns
    * @group Map Type
    */
  implicit class MapColumnOps[K, V](
      private val map: MapColumn[K, V]
  ) {

    /**
      * Returns the value if exist of the key
      *
      * @group Map Type
      * @param key
      *   the required key
      * @return
      *   a DoricColumn of the type of value, can be null if the key doesn't
      *   exist.
      */
    def get(key: DoricColumn[K]): DoricColumn[V] =
      (map.elem, key.elem).mapN(_(_)).toDC

    /**
      * Returns an unordered array containing the keys of the map.
      *
      * @group Map Type
      * @return
      *   the DoricColumn of the Array of keys
      * @see [[org.apache.spark.sql.functions.map_keys]]
      */
    def keys: DoricColumn[Array[K]] =
      map.elem.map(map_keys).toDC

    /**
      * Returns an unordered array containing the values of the map.
      *
      * @group Map Type
      * @return
      *   the DoricColumn of the Array of values
      * @see [[org.apache.spark.sql.functions.map_values]]
      */
    def values: DoricColumn[Array[V]] =
      map.elem.map(map_values).toDC

    /**
      * Returns value for the given key in value.
      *
      * @group Map Type
      * @see [[org.apache.spark.sql.functions.element_at]]
      */
    def elementAt(key: DoricColumn[K]): DoricColumn[V] =
      elementAtAbstract(map, key)

    /**
      * Creates a new row for each element in the given map column.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.explode]]
      */
      // TODO
    def explode: DoricColumn[(K, V)] = map.elem.map(f.explode).toDC

    /**
      * Creates a new row for each element in the given map column.
      * Unlike explode, if the map is null or empty then null is produced.
      *
      * @group Array Type
      * @see [[org.apache.spark.sql.functions.explode_outer]]
      */
      // TODO
    def explodeOuter: DoricColumn[(K, V)] = map.elem.map(f.explode_outer).toDC

    /**
      * Returns length of map.
      *
      * The function returns null for null input if spark.sql.legacy.sizeOfNull is set to false or
      * spark.sql.ansi.enabled is set to true. Otherwise, the function returns -1 for null input.
      * With the default settings, the function returns -1 for null input.
      *
      * @group Map Type
      * @see [[org.apache.spark.sql.functions.size]]
      */
    def size: IntegerColumn = map.elem.map(f.size).toDC

    /**
      * Returns an unordered array of all entries in the given map.
      *
      * @group Map Type
      * @see [[org.apache.spark.sql.functions.map_entries]]
      */
      // TODO
    def mapEntries: ArrayColumn[(K, V)] = map.elem.map(f.map_entries).toDC

    /**
      * Returns a map whose key-value pairs satisfy a predicate.
      * @example {{{
      *   df.select(colMap("m").filter((k, v) => k * 10 === v))
      * }}}
      *
      * @group Map Type
      * @see [[org.apache.spark.sql.functions.map_filter]]
      */
    def filter(
        function: (DoricColumn[K], DoricColumn[V]) => BooleanColumn
    ): MapColumn[K, V] = {
      (map.elem, function(x, y).elem).mapN { (a, f) =>
        new Column(MapFilter(a.expr, lam2(f.expr)))
      }.toDC
    }

    /**
      * Merge two given maps, key-wise into a single map using a function.
      * @example {{{
      *   df.select(colMap("m1").zipWith(col("m2"), (k, v1, v2) => k === v1 + v2))
      * }}}
      *
      * @group Map Type
      * @see [[org.apache.spark.sql.functions.map_zip_with]]
      */
    def zipWith[V2, R](
        map2: MapColumn[K, V2],
        function: (
            DoricColumn[K],
            DoricColumn[V],
            DoricColumn[V2]
        ) => DoricColumn[R]
    ): MapColumn[K, R] = {
      (map.elem, map2.elem, function(x, y, z).elem).mapN { (a, b, f) =>
        new Column(MapZipWith(a.expr, b.expr, lam3(f.expr)))
      }.toDC
    }

    /**
      * Applies a function to every key-value pair in a map and returns
      * a map with the results of those applications as the new keys for the pairs.
      * @example {{{
      *   df.select(colMap("m").transformKeys((k, v) => k + v))
      * }}}
      *
      * @group Map Type
      * @see [[org.apache.spark.sql.functions.transform_keys]]
      */
    def transformKeys[K2](
        function: (DoricColumn[K], DoricColumn[V]) => DoricColumn[K2]
    ): MapColumn[K2, V] = {
      (map.elem, function(x, y).elem).mapN { (a, f) =>
        new Column(TransformKeys(a.expr, lam2(f.expr)))
      }.toDC
    }

    /**
      * Applies a function to every key-value pair in a map and returns
      * a map with the results of those applications as the new values for the pairs.
      * @example {{{
      *   df.select(colMap("m").transformValues((k, v) => k + v))
      * }}}
      *
      * @group Map Type
      * @see [[org.apache.spark.sql.functions.transform_values]]
      */
    def transformValues[V2](
        function: (DoricColumn[K], DoricColumn[V]) => DoricColumn[V2]
    ): MapColumn[K, V2] = {
      (map.elem, function(x, y).elem).mapN { (a, f) =>
        new Column(TransformValues(a.expr, lam2(f.expr)))
      }.toDC
    }

  }
}
