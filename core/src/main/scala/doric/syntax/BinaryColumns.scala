package doric
package syntax

import doric.types.{BinaryType, SparkType}
import org.apache.spark.sql.{functions => f}

private[syntax] trait BinaryColumns {

  implicit class BinaryOperationsSyntax[T: BinaryType: SparkType](
      column: DoricColumn[T]
  ) {

    /**
      * Calculates the MD5 digest of a binary column and returns the value
      * as a 32 character hex string.
      *
      * @group Binary Type
      */
    def md5: StringColumn = column.elem.map(f.md5).toDC

    /**
      * Calculates the SHA-1 digest of a binary column and returns the value
      * as a 40 character hex string.
      *
      * @group Binary Type
      */
    def sha1: StringColumn = column.elem.map(f.sha1).toDC

    /**
      * Calculates the SHA-2 family of hash functions of a binary column and
      * returns the value as a hex string.
      *
      * @group Binary Type
      */
    def sha2(numBits: Int): StringColumn =
      column.elem.map(x => f.sha2(x, numBits)).toDC

    /**
      * Calculates the cyclic redundancy check value (CRC32) of a binary column and
      * returns the value as a long column.
      *
      * @group Binary Type
      */
    def crc32: LongColumn = column.elem.map(f.crc32).toDC
  }

}
