package doric
package syntax

import cats.implicits.{catsSyntaxTuple2Semigroupal, catsSyntaxTuple3Semigroupal, catsSyntaxTuple4Semigroupal, toTraverseOps}

import org.apache.spark.sql.{Column, functions => f}
import org.apache.spark.sql.catalyst.expressions._

private[syntax] trait StringColumns {

  /**
    * Concatenate string columns to form a single one
    *
    * @group String Type
    * @param cols
    *   the String DoricColumns to concatenate
    * @return
    *   a reference of a single DoricColumn with all strings concatenated. If at
    *   least one is null will return null.
    */
  def concat(cols: StringColumn*): StringColumn =
    cols.map(_.elem).toList.sequence.map(f.concat(_: _*)).toDC

  /**
    * Concatenates multiple input string columns together into a single string column,
    * using the given separator.
    *
    * @note even if `cols` contain null columns, it prints remaining string columns (or empty string).
    * @example {{{
    * df.withColumn("res", concatWs("-".lit, col("col1"), col("col2")))
    *   .show(false)
    *     +----+----+----+
    *     |col1|col2| res|
    *     +----+----+----+
    *     |   1|   1| 1-1|
    *     |null|   2|   2|
    *     |   3|null|   3|
    *     |null|null|    |
    *     +----+----+----+
    * }}}
    * @group String Type
    * @see [[org.apache.spark.sql.functions.concat_ws]]
    */
  def concatWs(sep: StringColumn, cols: StringColumn*): StringColumn =
    (sep +: cols)
      .map(_.elem)
      .toList
      .sequence
      .map(l => {
        new Column(ConcatWs(l.map(_.expr)))
      })
      .toDC

  /**
    * Formats the arguments in printf-style and returns the result as a string
    * column.
    *
    * @group String Type
    * @param format
    *   Printf format
    * @param arguments
    *   the String DoricColumns to format
    * @return
    *   Formats the arguments in printf-style and returns the result as a string
    *   column.
    */
  def formatString(
      format: StringColumn,
      arguments: DoricColumn[_]*
  ): StringColumn =
    (format.elem, arguments.toList.traverse(_.elem))
      .mapN((f, args) => {
        new Column(FormatString((f +: args).map(_.expr): _*))
      })
      .toDC

  /**
    * Creates a string column for the file name of the current Spark task.
    *
    * @group String Type
    * @see [[org.apache.spark.sql.functions.input_file_name]]
    */
  def inputFileName(): StringColumn = DoricColumn(f.input_file_name)

  /**
    * Creates a string column for the file name of the current Spark task.
    *
    * @group String Type
    * @see [[inputFileName]]
    */
  @inline def sparkTaskName(): StringColumn = inputFileName()

  /**
    * Unique column operations
    *
    * @param s
    *   Doric String column
    */
  implicit class StringOperationsSyntax(s: DoricColumn[String]) {

    /**
      * ********************************************************
      *             SPARK SQL EQUIVALENT FUNCTIONS
      * ********************************************************
      */

    /**
      * Computes the numeric value of the first character of the string column,
      * and returns the result as an int column.
      *
      * @group String Type
      */
    def ascii: IntegerColumn = s.elem.map(f.ascii).toDC

    /**
      * Returns a new string column by converting the first letter of each word
      * to uppercase. Words are delimited by whitespace.
      *
      * For example, "hello world" will become "Hello World".
      *
      * @group String Type
      */
    def initcap: StringColumn = s.elem.map(f.initcap).toDC

    /**
      * Locate the position of the first occurrence of substr column in the
      * given string. Returns null if either of the arguments are null.
      *
      * @group String Type
      * @note
      *   The position is not zero based, but 1 based index. Returns 0 if substr
      *   could not be found in str.
      */
    def instr(substring: StringColumn): IntegerColumn =
      (s.elem, substring.elem)
        .mapN((str, substr) => {
          new Column(StringInstr(str.expr, substr.expr))
        })
        .toDC

    /**
      * Computes the character length of a given string or number of bytes of a
      * binary string. The length of character strings include the trailing
      * spaces. The length of binary strings includes binary zeros.
      *
      * @group String Type
      */
    def length: IntegerColumn = s.elem.map(f.length).toDC

    /**
      * Computes the Levenshtein distance of the two given string columns.
      *
      * @group String Type
      */
    def levenshtein(dc: StringColumn): IntegerColumn =
      (s.elem, dc.elem).mapN(f.levenshtein).toDC

    /**
      * Locate the position of the first occurrence of substr in a string
      * column, after position pos.
      *
      * @group String Type
      * @note
      *   The position is not zero based, but 1 based index. returns 0 if substr
      *   could not be found in str.
      */
    def locate(
        substr: StringColumn,
        pos: IntegerColumn = 1.lit
    ): IntegerColumn =
      (substr.elem, s.elem, pos.elem)
        .mapN((substring, str, position) => {
          new Column(StringLocate(substring.expr, str.expr, position.expr))
        })
        .toDC

    /**
      * Converts a string column to lower case.
      *
      * @group String Type
      */
    def lower: StringColumn = s.elem.map(f.lower).toDC

    /**
      * Left-pad the string column with pad to a length of len. If the string
      * column is longer than len, the return value is shortened to len
      * characters.
      *
      * @group String Type
      */
    def lpad(len: IntegerColumn, pad: StringColumn): StringColumn =
      (s.elem, len.elem, pad.elem)
        .mapN((str, lenCol, lpad) => {
          new Column(StringLPad(str.expr, lenCol.expr, lpad.expr))
        })
        .toDC

    /**
      * Trim the spaces from left end for the specified string value.
      *
      * @group String Type
      */
    def ltrim: StringColumn = s.elem.map(f.ltrim).toDC

    /**
      * Trim the specified character string from left end for the specified
      * string column.
      *
      * @group String Type
      */
    def ltrim(trimString: StringColumn): StringColumn =
      (s.elem, trimString.elem)
        .mapN((str, trimStr) => {
          new Column(StringTrimLeft(str.expr, trimStr.expr))
        })
        .toDC

    /**
      * Overlay the specified portion of `src` with `replace`, starting from
      * byte position `pos` of `src` and proceeding for `len` bytes.
      *
      * @group String Type
      */
    def overlay(
        replace: StringColumn,
        pos: IntegerColumn,
        len: IntegerColumn = (-1).lit
    ): StringColumn =
      (s.elem, replace.elem, pos.elem, len.elem).mapN(f.overlay).toDC

    /**
      * Extract a specific group matched by a Java regex, from the specified
      * string column. If the regex did not match, or the specified group did
      * not match, an empty string is returned. if the specified group index
      * exceeds the group count of regex, an IllegalArgumentException will be
      * thrown.
      *
      * @throws java.lang.IllegalArgumentException if the specified group index exceeds the group count of regex
      * @group String Type
      */
    def regexpExtract(
        exp: StringColumn,
        groupIdx: IntegerColumn
    ): StringColumn =
      (s.elem, exp.elem, groupIdx.elem)
        .mapN((str, regexp, gIdx) =>
          new Column(RegExpExtract(str.expr, regexp.expr, gIdx.expr))
        )
        .toDC

    /**
      * Replace all substrings of the specified string value that match regexp
      * with replacement.
      *
      * @group String Type
      */
    def regexpReplace(
        pattern: StringColumn,
        replacement: StringColumn
    ): StringColumn =
      (s.elem, pattern.elem, replacement.elem).mapN(f.regexp_replace).toDC

    /**
      * Repeats a string column n times, and returns it as a new string column.
      *
      * @group String Type
      */
    def repeat(n: IntegerColumn): StringColumn = (s.elem, n.elem)
      .mapN((str, times) => new Column(StringRepeat(str.expr, times.expr)))
      .toDC

    /**
      * Right-pad the string column with pad to a length of len. If the string
      * column is longer than len, the return value is shortened to len
      * characters.
      *
      * @group String Type
      */
    def rpad(len: IntegerColumn, pad: StringColumn): StringColumn =
      (s.elem, len.elem, pad.elem)
        .mapN((str, l, p) => new Column(StringRPad(str.expr, l.expr, p.expr)))
        .toDC

    /**
      * Trim the spaces from right end for the specified string value.
      *
      * @group String Type
      */
    def rtrim: StringColumn = s.elem.map(f.rtrim).toDC

    /**
      * Trim the specified character string from right end for the specified
      * string column.
      *
      * @group String Type
      */
    def rtrim(trimString: StringColumn): StringColumn =
      (s.elem, trimString.elem)
        .mapN((str, t) => new Column(StringTrimRight(str.expr, t.expr)))
        .toDC

    /**
      * Returns the soundex code for the specified expression.
      *
      * @group String Type
      */
    def soundex: StringColumn = s.elem.map(f.soundex).toDC

    /**
      * Splits str around matches of the given pattern.
      *
      * @param pattern
      *   a string representing a regular expression. The regex string should be
      *   a Java regular expression.
      *
      * @group String Type
      */
    def split(pattern: StringColumn): ArrayColumn[String] =
      split(pattern, (-1).lit)

    /**
      * Splits str around matches of the given pattern.
      *
      * @group String Type
      * @param pattern
      *   a string representing a regular expression. The regex string should be
      *   a Java regular expression.
      * @param limit
      *   an integer expression which controls the number of times the regex is
      *   applied. <ul> <li>limit greater than 0: The resulting array's length
      *   will not be more than limit, and the resulting array's last entry will
      *   contain all input beyond the last matched regex.</li> <li>limit less
      *   than or equal to 0: `regex` will be applied as many times as possible,
      *   and the resulting array can be of any size.</li> </ul>
      */
    def split(
        pattern: StringColumn,
        limit: IntegerColumn
    ): ArrayColumn[String] =
      (s.elem, pattern.elem, limit.elem)
        .mapN((str, p, l) => new Column(StringSplit(str.expr, p.expr, l.expr)))
        .toDC

    /**
      * Substring starts at `pos` and is of length `len` when str is String type
      * or returns the slice of byte array that starts at `pos` in byte and is
      * of length `len` when str is Binary type
      *
      * @group String Type
      * @note
      *   The position is not zero based, but 1 based index.
      */
    def substring(pos: IntegerColumn, len: IntegerColumn): StringColumn =
      (s.elem, pos.elem, len.elem)
        .mapN((str, p, l) => new Column(Substring(str.expr, p.expr, l.expr)))
        .toDC

    /**
      * Returns the substring from string str before count occurrences of the
      * delimiter delim. If count is positive, everything the left of the final
      * delimiter (counting from left) is returned. If count is negative, every
      * to the right of the final delimiter (counting from the right) is
      * returned. substring_index performs a case-sensitive match when searching
      * for delim.
      *
      * @group String Type
      */
    def substringIndex(
        delim: StringColumn,
        count: IntegerColumn
    ): StringColumn =
      (s.elem, delim.elem, count.elem)
        .mapN((str, d, c) =>
          new Column(SubstringIndex(str.expr, d.expr, c.expr))
        )
        .toDC

    /**
      * Translate any character in the src by a character in replaceString. The
      * characters in replaceString correspond to the characters in
      * matchingString. The translate will happen when any character in the
      * string matches the character in the `matchingString`.
      *
      * @group String Type
      */
    def translate(
        matchingString: StringColumn,
        replaceString: StringColumn
    ): StringColumn =
      (s.elem, matchingString.elem, replaceString.elem)
        .mapN((str, m, r) =>
          new Column(StringTranslate(str.expr, m.expr, r.expr))
        )
        .toDC

    /**
      * Trim the spaces from both ends for the specified string column.
      *
      * @group String Type
      */
    def trim: StringColumn = s.elem.map(f.trim).toDC

    /**
      * Trim the specified character from both ends for the specified string
      * column (literal).
      *
      * @group String Type
      */
    def trim(trimString: StringColumn): StringColumn =
      (s.elem, trimString.elem)
        .mapN((str, trimStr) => {
          new Column(StringTrim(str.expr, trimStr.expr))
        })
        .toDC

    /**
      * Converts a string column to upper case.
      *
      * @group String Type
      */
    def upper: StringColumn = s.elem.map(f.upper).toDC

    /**
      * ********************************************************
      *                     COLUMN FUNCTIONS
      * ********************************************************
      */

    /**
      * Contains the other element. Returns a boolean column based on a string
      * match.
      *
      * @group String Type
      */
    def contains(dc: StringColumn): BooleanColumn =
      (s.elem, dc.elem).mapN(_.contains(_)).toDC

    /**
      * String ends with. Returns a boolean column based on a string match.
      *
      * @group String Type
      */
    def endsWith(dc: StringColumn): BooleanColumn =
      (s.elem, dc.elem).mapN(_.endsWith(_)).toDC

    /**
      * SQL like expression. Returns a boolean column based on a SQL LIKE match.
      *
      * @group String Type
      */
    def like(literal: StringColumn): BooleanColumn =
      (s.elem, literal.elem)
        .mapN((str, l) => new Column(new Like(str.expr, l.expr)))
        .toDC

    /**
      * SQL RLIKE expression (LIKE with Regex). Returns a boolean column based
      * on a regex match.
      *
      * @group String Type
      */
    def rLike(literal: StringColumn): BooleanColumn =
      (s.elem, literal.elem)
        .mapN((str, regex) => new Column(RLike(str.expr, regex.expr)))
        .toDC

    /**
      * String starts with. Returns a boolean column based on a string match.
      *
      * @group String Type
      */
    def startsWith(dc: StringColumn): BooleanColumn =
      (s.elem, dc.elem).mapN(_.startsWith(_)).toDC

    /**
      * Same as rLike doric function.
      *
      * SQL RLIKE expression (LIKE with Regex). Returns a boolean column based
      * on a regex match.
      *
      * @group String Type
      */
    def matchRegex(literal: StringColumn): BooleanColumn = rLike(literal)

    /**
      * Computes the first argument into a binary from a string using the provided character set
      * (one of 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16').
      * If either argument is null, the result will also be null.
      *
      * @group String Type
      */
    def encode(charset: StringColumn): BinaryColumn =
      (s.elem, charset.elem)
        .mapN((col, char) => {
          new Column(Encode(col.expr, char.expr))
        })
        .toDC

    /**
      * Decodes a BASE64 encoded string column and returns it as a binary column.
      * This is the reverse of base64.
      *
      * @group String Type
      */
    def unbase64: BinaryColumn = s.elem.map(f.unbase64).toDC

    /**
      * Converts date/timestamp to Unix timestamp (in seconds),
      * using the default timezone and the default locale.
      *
      * @return
      *   A long
      *
      * @group String Type
      */
    def unixTimestamp: LongColumn = s.elem.map(f.unix_timestamp).toDC

    /**
      * Converts date/timestamp with given pattern to Unix timestamp (in seconds).
      *
      * @return
      *   A long, or null if the input was a string not of the correct format
      * @throws java.lang.IllegalArgumentException if invalid pattern
      *
      * @group String Type
      */
    def unixTimestamp(pattern: StringColumn): LongColumn =
      (s.elem, pattern.elem)
        .mapN((c, p) => {
          new Column(UnixTimestamp(c.expr, p.expr))
        })
        .toDC

    /**
      * ********************************************************
      *                     DORIC FUNCTIONS
      * ********************************************************
      */

    /**
      * Similar to concat doric function, but only with two columns
      *
      * @group String Type
      */
    def +(s2: StringColumn): StringColumn = concat(s, s2)

    /**
      * Converts the column into a `DateType` with a specified format
      *
      * See <a
      * href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">
      * Datetime Patterns</a> for valid date and time format patterns
      *
      * @group String Type
      * @param format
      *   A date time pattern detailing the format of `e` when `e`is a string
      * @return
      *   A date, or null if `e` was a string that could not be cast to a date
      *   or `format` was an invalid format
      */
    def toDate(format: StringColumn): LocalDateColumn =
      (s.elem, format.elem)
        .mapN((str, dateFormat) =>
          new Column(new ParseToDate(str.expr, dateFormat.expr))
        )
        .toDC

    /**
      * Converts time string with the given pattern to timestamp.
      *
      * See <a
      * href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">
      * Datetime Patterns</a> for valid date and time format patterns
      *
      * @group String Type
      * @param format
      *   A date time pattern detailing the format of `s` when `s` is a string
      * @return
      *   A timestamp, or null if `s` was a string that could not be cast to a
      *   timestamp or `format` was an invalid format
      */
    def toTimestamp(format: StringColumn): InstantColumn =
      (s.elem, format.elem)
        .mapN((str, tsFormat) =>
          new Column(new ParseToTimestamp(str.expr, tsFormat.expr))
        )
        .toDC

    /**
      * ********************************************************
      *                     MISC FUNCTIONS
      * ********************************************************
      */

    /**
      * Throws an exception with the provided error message.
      *
      * @throws java.lang.RuntimeException with the error message
      * @group String Type
      */
    def raiseError: NullColumn = s.elem.map(f.raise_error).toDC
  }
}
