package doric
package syntax

import cats.implicits._
import doric.types.{SparkType}
import org.apache.spark.sql.functions.{map_keys, map_values}
import org.apache.spark.sql.{Column, Row, functions => f}

import scala.jdk.CollectionConverters._

protected trait MapColumns {

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
  implicit class MapColumnOps[K: SparkType, V: SparkType](
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
      * DORIC EXCLUSIVE! Map to array conversion
      */
    def toArray: DoricColumn[Array[Row]] =
      map.keys.zipWith(map.values)((a, b) => struct(a.as("key"), b.as("value")))

    /**
      * Creates a new row for each element in the given map column.
      *
      * @note WARNING: Unlike spark, doric returns a struct
      * @example {{{
      *     ORIGINAL             SPARK         DORIC
      *  +----------------+   +---+-----+     +------+
      *  |col             |   |key|value|     |col   |
      *  +----------------+   +---+-----+     +------+
      *  |[a -> b, c -> d]|   |a  |b    |     |{a, b}|
      *  |[]              |   |c  |d    |     |{c, d}|
      *  |null            |   +---+-----+     +------+
      *  +----------------+
      * }}}
      *
      * @group Map Type
      * @see [[org.apache.spark.sql.functions.explode]]
      */
    def explode: DoricColumn[Row] = map.toArray.elem.map(f.explode).toDC

    /**
      * Creates a new row for each element in the given map column.
      * @note Unlike explode, if the array is null or empty then null is produced.
      * @note WARNING: Unlike spark, doric returns a struct
      * @example {{{
      *        ORIGINAL               SPARK            DORIC
      *  +---+----------------+   +---+----+-----+  +---+------+
      *  |ix |col             |   |ix |key |value|  |ix |col   |
      *  +---+----------------+   +---+----+-----+  +---+------+
      *  |1  |{a -> b, c -> d}|   |1  |a   |b    |  |1  |{a, b}|
      *  |2  |{}              |   |1  |c   |d    |  |1  |{c, d}|
      *  |3  |null            |   |2  |null|null |  |2  |null  |
      *  +---+----------------+   |3  |null|null |  |3  |null  |
      *                           +---+----+-----+  +---+------+
      * }}}
      *
      * @group Map Type
      * @see [[org.apache.spark.sql.functions.explode_outer]]
      */
    def explodeOuter: DoricColumn[Row] =
      map.toArray.elem.map(f.explode_outer).toDC

    private def mapToArrayZipped: DoricColumn[Array[Row]] =
      map.toArray.transformWithIndex((value, index) =>
        struct(
          index.asCName("pos".cname),
          value.getChild[K]("key").asCName("key".cname),
          value.getChild[V]("value").asCName("value".cname)
        )
      )

    /**
      * Creates a new row for each element with position in the given map column.
      *
      * @note Uses the default column name pos for position, and key and value for elements in the map.
      * @note WARNING: Unlike spark, doric returns a struct
      * @example {{{
      *     ORIGINAL               SPARK             DORIC
      *  +----------------+   +---+---+-----+     +---------+
      *  |col             |   |pos|key|value|     |col      |
      *  +----------------+   +---+---+-----+     +---------+
      *  |[a -> b, c -> d]|   |1  |a  |b    |     |{1, a, b}|
      *  |[]              |   |2  |c  |d    |     |{2, c, d}|
      *  |null            |   +---+---+-----+     +---------+
      *  +----------------+
      * }}}
      *
      * @group Map Type
      * @see [[org.apache.spark.sql.functions.posexplode]]
      */
    def posExplode: DoricColumn[Row] = mapToArrayZipped.explode

    /**
      * Creates a new row for each element with position in the given map column.
      * Unlike posexplode, if the map is null or empty then the row (null, null) is produced.
      *
      * @note Uses the default column name pos for position, and key and value for elements in the map.
      * @note WARNING: Unlike spark, doric returns a struct
      * @example {{{
      *     ORIGINAL               SPARK             DORIC
      *  +----------------+   +---+----+-----+    +---------+
      *  |col             |   |pos|key |value|    |col      |
      *  +----------------+   +---+----+-----+    +---------+
      *  |[a -> b, c -> d]|   |1  |a   |b    |    |{1, a, b}|
      *  |[]              |   |2  |c   |d    |    |{2, c, d}|
      *  |null            |   |2  |null|null |    |null     |
      *  +----------------+   |3  |null|null |    |null     |
      *                       +---+----+-----+    +---+-----+
      * }}}
      *
      * @group Map Type
      * @see [[org.apache.spark.sql.functions.posexplode_outer]]
      */
    def posExplodeOuter: RowColumn = mapToArrayZipped.explodeOuter

    /**
      * Converts a column containing a StructType into a JSON string with the specified schema.
      * @throws java.lang.IllegalArgumentException in the case of an unsupported type.
      *
      * @group Map Type
      * @see org.apache.spark.sql.functions.to_json(e:org\.apache\.spark\.sql\.Column,options:scala\.collection\.immutable\.Map\[java\.lang\.String,java\.lang\.String\]):* org.apache.spark.sql.functions.to_csv
      * @todo scaladoc link (issue #135)
      */
    def toJson(options: Map[String, String] = Map.empty): StringColumn =
      map.elem.map(x => f.to_json(x, options.asJava)).toDC
  }
}
