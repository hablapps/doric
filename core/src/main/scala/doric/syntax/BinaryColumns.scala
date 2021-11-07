package doric
package syntax

import cats.implicits.catsSyntaxTuple2Semigroupal

import doric.types.{BinaryType, SparkType}

import org.apache.spark.sql.catalyst.expressions.Decode
import org.apache.spark.sql.{Column, functions => f}

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

    /**
      * Computes the BASE64 encoding of a binary column and returns it as a string column.
      * This is the reverse of unbase64.
      *
      * @group Binary Type
      */
    def base64: StringColumn = column.elem.map(f.base64).toDC

    /**
      * Computes the first argument into a string from a binary using the provided character set
      * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
      * If either argument is null, the result will also be null.
      *
      * @group Binary Type
      */
    def decode(charset: StringColumn): StringColumn =
      (column.elem, charset.elem)
        .mapN((col, char) => {
          new Column(Decode(col.expr, char.expr))
        })
        .toDC
  }

}
