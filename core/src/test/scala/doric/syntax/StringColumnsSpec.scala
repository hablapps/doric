package doric
package syntax

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Row, functions => f}

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneId}
import scala.jdk.CollectionConverters._

class StringColumnsSpec extends DoricTestElements {

  describe("concat doric function") {
    import spark.implicits._

    val df = List(("1", "1"), (null, "2"), ("3", null), (null, null))
      .toDF("col1", "col2")

    it("should work as spark concat function") {
      df.testColumns2("col1", "col2")(
        (col1, col2) => concat(colString(col1), colString(col2)),
        (col1, col2) => f.concat(f.col(col1), f.col(col2)),
        List("11", null, null, null).map(Option(_))
      )
    }

    it("should work with + function") {
      df.testColumns3("col1", "col2", "col1")(
        (col1, col2, col3) =>
          colString(col1) + colString(col2) + colString(col3),
        (col1, col2, col3) => f.concat(f.col(col1), f.col(col2), f.col(col3)),
        List("111", null, null, null).map(Option(_))
      )
    }
  }

  describe("concatWs doric function") {
    import spark.implicits._

    val df = List(("1", "1"), (null, "2"), ("3", null), (null, null))
      .toDF("col1", "col2")

    it("should work as spark concat_ws function") {
      df.testColumns3("-", "col1", "col2")(
        (sep, col1, col2) => concatWs(sep.lit, col(col1), col(col2)),
        (sep, col1, col2) => f.concat_ws(sep, f.col(col1), f.col(col2)),
        List("1-1", "2", "3", "").map(Option(_))
      )
    }
  }

  describe("formatString doric function") {
    import spark.implicits._

    it("should work as spark formatString function") {
      val df = List(("1", Some(1)), (null, Some(2)), ("3", None), (null, None))
        .toDF("col1", "col2")

      df.testColumns4("Hello World %s %s %d", "col1", "col1", "col2")(
        (format, col1, col2, col3) =>
          formatString(
            format.lit,
            colString(col1),
            colString(col2),
            colInt(col3)
          ),
        (format, col1, col2, col3) =>
          f.format_string(format, f.col(col1), f.col(col2), f.col(col3)),
        List(
          "Hello World 1 1 1",
          "Hello World null null 2",
          "Hello World 3 3 null",
          "Hello World null null null"
        )
          .map(Option(_))
      )
    }
  }

  describe("inputFileName doric function") {
    import spark.implicits._
    val df = List("abc", null).toDF("col1")

    it("should work as spark input_file_name function") {
      df.testColumns("col1")(
        _ => inputFileName(),
        _ => f.input_file_name()
      )
    }

    it("should work as spark input_file_name function using sparkTaskName") {
      df.testColumns("col1")(
        _ => sparkTaskName(),
        _ => f.input_file_name()
      )
    }
  }

  describe("ascii doric function") {
    import spark.implicits._

    it("should work as spark ascii function") {
      val df = List("1", "a", "A", null).toDF("col1")

      df.testColumns("col1")(
        c => colString(c).ascii,
        c => f.ascii(f.col(c)),
        List(
          Some(49),
          Some(97),
          Some(65),
          None
        )
      )
    }
  }

  describe("initcap doric function") {
    import spark.implicits._

    it("should work as spark initcap function") {
      val df = List("hello world", "ñañeñiño", "Tú vas a ir a Álaba", "1", null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colString(c).initCap,
        c => f.initcap(f.col(c)),
        List("Hello World", "Ñañeñiño", "Tú Vas A Ir A Álaba", "1", null)
          .map(Option(_))
      )
    }
  }

  describe("instr doric function") {
    import spark.implicits._

    it("should work as spark instr function") {
      val df = List("hello world", "ñañeñiño", "Tú vas a ir a Álaba", "1", null)
        .toDF("col1")

      df.testColumns2("col1", "a")(
        (c, str) => colString(c).inStr(str.lit),
        (c, str) => f.instr(f.col(c), str),
        List(Some(0), Some(2), Some(5), Some(0), None)
      )
    }
  }

  describe("length doric function") {
    import spark.implicits._

    it("should work as spark length function") {
      val df = List("hello world", "ñañeñiño", "Tú vas a ir a Álaba", "1", null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colString(c).length,
        c => f.length(f.col(c)),
        List(Some(11), Some(8), Some(19), Some(1), None)
      )
    }
  }

  describe("levenshtein doric function") {
    import spark.implicits._

    it("should work as spark levenshtein function") {
      val df = List(
        ("kitten", "sitting"),
        ("jander", ""),
        (null, "jander"),
        ("jander", null),
        (null, null)
      ).toDF("col1", "col2")

      df.testColumns2("col1", "col2")(
        (c, right) => colString(c).levenshtein(colString(right)),
        (c, right) => f.levenshtein(f.col(c), f.col(right)),
        List(Some(3), Some(6), None, None, None)
      )
    }
  }

  describe("locate doric function") {
    import org.apache.spark.sql.functions.{locate => sparkLocate}
    import spark.implicits._

    val df = List("hello world", "abcde hello hello", "other words", null)
      .toDF("col1")

    it("should work as spark locate function with default position") {
      df.testColumns2("col1", "hello")(
        (c, substr) => colString(c).locate(substr.lit),
        (c, substr) => sparkLocate(substr, f.col(c)),
        List(Some(1), Some(7), Some(0), None)
      )
    }

    it("should work as spark locate function setting a position") {
      df.testColumns3("col1", "hello", 4)(
        (c, substr, pos) => colString(c).locate(substr.lit, pos.lit),
        (c, substr, pos) => sparkLocate(substr, f.col(c), pos),
        List(Some(0), Some(7), Some(0), None)
      )
    }
  }

  describe("lower doric function") {
    import spark.implicits._

    it("should work as spark lower function") {
      val df = List("Hello World", "HELLO WORLD", " 1", null).toDF("col1")

      df.testColumns("col1")(
        c => colString(c).lower,
        c => f.lower(f.col(c)),
        List("hello world", "hello world", " 1", null).map(Option(_))
      )
    }
  }

  describe("lpad doric function") {
    import spark.implicits._

    val df = List("abcd", "", "1", null).toDF("col1")

    it("should work as spark lpad function") {
      df.testColumns3("col1", 7, ".")(
        (c, len, pad) => colString(c).lpad(len.lit, pad.lit),
        (c, len, pad) => f.lpad(f.col(c), len, pad),
        List("...abcd", "." * 7, "." * 6 + "1", null).map(Option(_))
      )
    }

    it("should cut the text if lpad length < than text length") {
      df.testColumns3("col1", 0, ".")(
        (c, len, pad) => colString(c).lpad(len.lit, pad.lit),
        (c, len, pad) => f.lpad(f.col(c), len, pad),
        List("", "", "", null).map(Option(_))
      )
    }

    it("should do nothing if pad is empty") {
      df.testColumns3("col1", 7, "")(
        (c, len, pad) => colString(c).lpad(len.lit, pad.lit),
        (c, len, pad) => f.lpad(f.col(c), len, pad),
        List("abcd", "", "1", null).map(Option(_))
      )
    }
  }

  describe("ltrim doric function") {
    import spark.implicits._

    it("should work as spark ltrim function") {
      val df = List("  hello world  ", "hello world  ", "  hello world", null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colString(c).ltrim,
        c => f.ltrim(f.col(c)),
        List("hello world  ", "hello world  ", "hello world", null).map(
          Option(_)
        )
      )
    }

    it("should work as spark ltrim function with trim argument") {
      val df =
        List("--hello world--", "hello world--", "--hello world", null).toDF(
          "col1"
        )

      df.testColumns2("col1", "-")(
        (c, t) => colString(c).ltrim(t.lit),
        (c, t) => f.ltrim(f.col(c), t),
        List("hello world--", "hello world--", "hello world", null).map(
          Option(_)
        )
      )
    }

    it("should do nothing if empty trimString") {
      val df =
        List("--hello world--", "hello world--", "--hello world", null)
          .toDF("col1")

      df.testColumns2("col1", "")(
        (c, t) => colString(c).ltrim(t.lit),
        (c, t) => f.ltrim(f.col(c), t),
        List("--hello world--", "hello world--", "--hello world", null).map(
          Option(_)
        )
      )
    }
  }

  describe("regexpExtract doric function") {
    import spark.implicits._

    val df = List("100-200", "100-a", null).toDF("col1")

    it("should work as spark regexpExtract function") {
      df.testColumns3("col1", "(\\d+)-(\\d+)", 1)(
        (str, regex, group) =>
          colString(str).regexpExtract(regex.lit, group.lit),
        (str, regex, group) => f.regexp_extract(f.col(str), regex, group),
        List("100", "", null).map(Option(_))
      )
    }

    it("should work as spark regexpExtract function if regex is empty") {
      df.testColumns3("col1", "", 0)(
        (str, regex, group) =>
          colString(str).regexpExtract(regex.lit, group.lit),
        (str, regex, group) => f.regexp_extract(f.col(str), regex, group),
        List("", "", null).map(Option(_))
      )
    }

    if (spark.version >= "2.4.6") {
      it("should raise an error if group > regex group result") {
        intercept[java.lang.IllegalArgumentException] {
          df.withColumn(
            "res",
            colString("col1").regexpExtract("(\\d+)-(\\d+)".lit, 4.lit)
          ).collect()
        }
      }
    }
  }

  describe("regexpReplace doric function") {
    import spark.implicits._

    val df = List(
      ("hello world", "world", "everybody"),
      ("hello world", null, "everybody"),
      ("hello world", "world", null),
      (null, "world", "everybody"),
      (null, null, null)
    ).toDF("str", "pattern", "replacement")

    it("should work as spark regexpReplace function") {
      df.testColumns3("str", "pattern", "replacement")(
        (str, p, r) => colString(str).regexpReplace(colString(p), colString(r)),
        (str, p, r) => f.regexp_replace(f.col(str), f.col(p), f.col(r)),
        List("hello everybody", null, null, null, null).map(Option(_))
      )
    }
  }

  describe("repeat doric function") {
    import spark.implicits._

    val df = List("hello world", "12345", null).toDF("col1")

    it("should work as spark repeat function") {
      df.testColumns2("col1", 2)(
        (str, repeat) => colString(str).repeat(repeat.lit),
        (str, repeat) => f.repeat(f.col(str), repeat),
        List("hello worldhello world", "1234512345", null).map(Option(_))
      )
    }

    it("should empty the string column if repeat = 0") {
      df.testColumns2("col1", 0)(
        (str, repeat) => colString(str).repeat(repeat.lit),
        (str, repeat) => f.repeat(f.col(str), repeat),
        List("", "", null).map(Option(_))
      )
    }
  }

  describe("rpad doric function") {
    import spark.implicits._

    val df = List("abcd", "", "1", null).toDF("col1")

    it("should work as spark rpad function") {
      df.testColumns3("col1", 7, ".")(
        (c, len, pad) => colString(c).rpad(len.lit, pad.lit),
        (c, len, pad) => f.rpad(f.col(c), len, pad),
        List("abcd...", "." * 7, "1" + "." * 6, null).map(Option(_))
      )
    }

    it("should cut the text if rpad length < than text length") {
      df.testColumns3("col1", 0, ".")(
        (c, len, pad) => colString(c).rpad(len.lit, pad.lit),
        (c, len, pad) => f.rpad(f.col(c), len, pad),
        List("", "", "", null).map(Option(_))
      )
    }

    it("should do nothing if pad is empty") {
      df.testColumns3("col1", 7, "")(
        (c, len, pad) => colString(c).rpad(len.lit, pad.lit),
        (c, len, pad) => f.rpad(f.col(c), len, pad),
        List("abcd", "", "1", null).map(Option(_))
      )
    }
  }

  describe("rtrim doric function") {
    import spark.implicits._

    it("should work as spark rtrim function") {
      val df = List("  hello world  ", "hello world  ", "  hello world", null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colString(c).rtrim,
        c => f.rtrim(f.col(c)),
        List("  hello world", "hello world", "  hello world", null).map(
          Option(_)
        )
      )
    }

    it("should work as spark rtrim function with trim argument") {
      val df =
        List("--hello world--", "hello world--", "--hello world", null).toDF(
          "col1"
        )

      df.testColumns2("col1", "-")(
        (c, t) => colString(c).rtrim(t.lit),
        (c, t) => f.rtrim(f.col(c), t),
        List("--hello world", "hello world", "--hello world", null).map(
          Option(_)
        )
      )
    }

    it("should do nothing if empty trimString") {
      val df =
        List("--hello world--", "hello world--", "--hello world", null)
          .toDF("col1")

      df.testColumns2("col1", "")(
        (c, t) => colString(c).rtrim(t.lit),
        (c, t) => f.rtrim(f.col(c), t),
        List("--hello world--", "hello world--", "--hello world", null).map(
          Option(_)
        )
      )
    }
  }

  describe("soundex doric function") {
    import spark.implicits._

    val df = List("hello world", "12345", null).toDF("col1")

    it("should work as spark soundex function") {
      df.testColumns("col1")(
        str => colString(str).soundex,
        str => f.soundex(f.col(str)),
        List("H464", "12345", null).map(Option(_))
      )
    }
  }

  describe("split doric function") {
    import spark.implicits._

    val df = List("hello world", "12345", null).toDF("col1")

    it("should work as spark split function") {
      df.testColumns2("col1", " ")(
        (str, pattern) => colString(str).split(pattern.lit),
        (str, pattern) => f.split(f.col(str), pattern),
        List(Array("hello", "world"), Array("12345"), null).map(Option(_))
      )
    }

    it("should split every char if no pattern is empty") {
      df.testColumns2("col1", "")(
        (str, pattern) => colString(str).split(pattern.lit),
        (str, pattern) => f.split(f.col(str), pattern),
        if (!spark.version.startsWith("3.4"))
          List(
            Array("h", "e", "l", "l", "o", " ", "w", "o", "r", "l", "d", ""),
            Array("1", "2", "3", "4", "5", ""),
            null
          ).map(Option(_))
        else
          List(
            Array("h", "e", "l", "l", "o", " ", "w", "o", "r", "l", "d"),
            Array("1", "2", "3", "4", "5"),
            null
          ).map(Option(_))
      )
    }
  }

  describe("substring doric function") {
    import spark.implicits._

    val df = List("hello world", "12345", null).toDF("col1")

    it("should work as spark substring function") {
      df.testColumns3("col1", 3, 5)(
        (str, pos, len) => colString(str).substring(pos.lit, len.lit),
        (str, pos, len) => f.substring(f.col(str), pos, len),
        List("llo w", "345", null).map(Option(_))
      )
    }

    it("should generate empty string if pos > length of string") {
      df.testColumns3("col1", 30, 5)(
        (str, pos, len) => colString(str).substring(pos.lit, len.lit),
        (str, pos, len) => f.substring(f.col(str), pos, len),
        List("", "", null).map(Option(_))
      )
    }

    it("should generate empty string if len = 0") {
      df.testColumns3("col1", 3, 0)(
        (str, pos, len) => colString(str).substring(pos.lit, len.lit),
        (str, pos, len) => f.substring(f.col(str), pos, len),
        List("", "", null).map(Option(_))
      )
    }
  }

  describe("substringIndex doric function") {
    import spark.implicits._

    val df = List("hello world", "12345", null).toDF("col1")

    it("should work as spark substringIndex function") {
      df.testColumns3("col1", " ", 1)(
        (str, delim, count) =>
          colString(str).substringIndex(delim.lit, count.lit),
        (str, delim, count) => f.substring_index(f.col(str), delim, count),
        List("hello", "12345", null).map(Option(_))
      )
    }

    it("should work as spark substringIndex function if count < 0") {
      df.testColumns3("col1", " ", -1)(
        (str, delim, count) =>
          colString(str).substringIndex(delim.lit, count.lit),
        (str, delim, count) => f.substring_index(f.col(str), delim, count),
        List("world", "12345", null).map(Option(_))
      )
    }

    it("should empty strings if delim is empty") {
      df.testColumns3("col1", "", 1)(
        (str, delim, count) =>
          colString(str).substringIndex(delim.lit, count.lit),
        (str, delim, count) => f.substring_index(f.col(str), delim, count),
        List("", "", null).map(Option(_))
      )
    }

    it("should empty strings if count = 0") {
      df.testColumns3("col1", " ", 0)(
        (str, delim, count) =>
          colString(str).substringIndex(delim.lit, count.lit),
        (str, delim, count) => f.substring_index(f.col(str), delim, count),
        List("", "", null).map(Option(_))
      )
    }
  }

  describe("translate doric function") {
    import spark.implicits._

    val df = List("hello world", "123456", null).toDF("col1")

    it("should work as spark translate function") {
      df.testColumns3("col1", "l wd2345eh", "L.Wd1111")(
        (str, matching, replace) =>
          colString(str).translate(matching.lit, replace.lit),
        (str, matching, replace) => f.translate(f.col(str), matching, replace),
        List("LLo.WorLd", "111116", null).map(Option(_))
      )
    }

    it("should do nothing id matching string is empty") {
      df.testColumns3("col1", "", "L.Wd1111")(
        (str, matching, replace) =>
          colString(str).translate(matching.lit, replace.lit),
        (str, matching, replace) => f.translate(f.col(str), matching, replace),
        List("hello world", "123456", null).map(Option(_))
      )
    }

    it("should remove characters if replace is empty") {
      df.testColumns3("col1", "l wd2345", "")(
        (str, matching, replace) =>
          colString(str).translate(matching.lit, replace.lit),
        (str, matching, replace) => f.translate(f.col(str), matching, replace),
        List("heoor", "16", null).map(Option(_))
      )
    }
  }

  describe("trim doric function") {
    import spark.implicits._

    val df = List("  hello world  ", "hello world  ", "  hello world", null)
      .toDF("col1")

    it("should work as spark trim function") {
      df.testColumns("col1")(
        c => colString(c).trim,
        c => f.trim(f.col(c)),
        List("hello world", "hello world", "hello world", null).map(Option(_))
      )
    }

    it("should work as spark trim function with trim argument") {
      df.testColumns2("col1", " ")(
        (c, t) => colString(c).trim(t.lit),
        (c, t) => f.trim(f.col(c), t),
        List("hello world", "hello world", "hello world", null).map(Option(_))
      )
    }

    it("should do nothing if trim argument is empty") {

      df.testColumns2("col1", "")(
        (c, t) => colString(c).trim(t.lit),
        (c, t) => f.trim(f.col(c), t),
        List("  hello world  ", "hello world  ", "  hello world", null).map(
          Option(_)
        )
      )
    }
  }

  describe("upper doric function") {
    import spark.implicits._

    val df = List("hello world", "123456", null)
      .toDF("col1")

    it("should work as spark upper function") {
      df.testColumns("col1")(
        c => colString(c).upper,
        c => f.upper(f.col(c)),
        List("HELLO WORLD", "123456", null).map(Option(_))
      )
    }
  }

  describe("contains doric function") {
    import spark.implicits._

    val df = List("hello world", "123456", null).toDF("col1")

    it("should work as spark contains function") {
      df.testColumns2("col1", "world")(
        (c, str) => colString(c).contains(str.lit),
        (c, str) => f.col(c).contains(str),
        List(Some(true), Some(false), None)
      )
    }
  }

  describe("endsWith doric function") {
    import spark.implicits._

    val df = List("hello world", "123456", null).toDF("col1")

    it("should work as spark endsWith function") {
      df.testColumns2("col1", "world")(
        (c, str) => colString(c).endsWith(str.lit),
        (c, str) => f.col(c).endsWith(str),
        List(Some(true), Some(false), None)
      )
    }
  }

  describe("like doric function") {
    import spark.implicits._

    val df = List("hello world", "123456", null).toDF("col1")

    it("should work as spark like function") {
      df.testColumns2("col1", "%45%")(
        (c, regex) => colString(c).like(regex.lit),
        (c, regex) => f.col(c).like(regex),
        List(Some(false), Some(true), None)
      )
    }
  }

  describe("rLike doric function") {
    import spark.implicits._

    val df = List("hello world", "123456", null).toDF("col1")

    it("should work as spark rlike function") {
      df.testColumns2("col1", "^[0-9]*$")(
        (c, regex) => colString(c).rLike(regex.lit),
        (c, regex) => f.col(c).rlike(regex),
        List(Some(false), Some(true), None)
      )
    }

    it("should work with matchRegex function") {
      df.testColumns2("col1", "^[0-9]*$")(
        (c, regex) => colString(c).matchRegex(regex.lit),
        (c, regex) => f.col(c).rlike(regex),
        List(Some(false), Some(true), None)
      )
    }
  }

  describe("startsWith doric function") {
    import spark.implicits._

    val df = List("hello world", "123456", null).toDF("col1")

    it("should work as spark startsWith function") {
      df.testColumns2("col1", "hello")(
        (c, str) => colString(c).startsWith(str.lit),
        (c, str) => f.col(c).startsWith(str),
        List(Some(true), Some(false), None)
      )
    }
  }

  describe("toDate doric function") {
    import spark.implicits._

    val df = List("28/05/2021", "28-05-21", null).toDF("col1")

    it(
      "should work as spark toDate function (returning null if malformed format)"
    ) {
      df.testColumns2("col1", "dd/MM/yyyy")(
        (c, str) => colString(c).toDate(str.lit),
        (c, str) => f.to_date(f.col(c), str),
        List(Some(LocalDate.of(2021, 5, 28)), None, None)
      )
    }

    it("should return none if empty format") {
      df.testColumns2("col1", "")(
        (c, str) => colString(c).toDate(str.lit),
        (c, str) => f.to_date(f.col(c), str),
        List(None, None, None)
      )
    }
  }

  describe("toTimestamp doric function") {
    import spark.implicits._

    val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
      .withZone(ZoneId.systemDefault())
    val expectedTime: Instant = Instant.parse("2021-05-28T10:55:23Z")
    val df = List(formatter.format(expectedTime), "28/05/21 04:27:31", null)
      .toDF("col1")

    it(
      "should work as spark toTimestamp function (returning null if malformed date)"
    ) {
      df.testColumns2("col1", "yyyy-MM-dd'T'hh:mm:ss")(
        (c, str) => colString(c).toTimestamp(str.lit),
        (c, str) => f.to_timestamp(f.col(c), str),
        List(Some(expectedTime), None, None)
      )
    }

    it("should return none if empty format") {
      df.testColumns2("col1", "")(
        (c, str) => colString(c).toTimestamp(str.lit),
        (c, str) => f.to_timestamp(f.col(c), str),
        List(None, None, None)
      )
    }
  }

  describe("encode doric function") {
    import spark.implicits._

    it("should work as spark encode function") {
      val df = List("this is a string", null)
        .toDF("col1")

      df.testColumns2("col1", "UTF-8")(
        (c, charset) => colString(c).encode(charset.lit),
        (c, charset) => f.encode(f.col(c), charset),
        List(
          Some(
            Array[Byte](116, 104, 105, 115, 32, 105, 115, 32, 97, 32, 115, 116,
              114, 105, 110, 103)
          ),
          None
        )
      )
    }
  }

  describe("unbase64 doric function") {
    import spark.implicits._

    it("should work as spark unbase64 function") {
      val df = List("AQIDBAU=", null)
        .toDF("col1")

      df.testColumns("col1")(
        c => colString(c).unbase64,
        c => f.unbase64(f.col(c)),
        List(
          Some(Array[Byte](1, 2, 3, 4, 5)),
          None
        )
      )
    }
  }

  describe("string unixTimestamp doric function") {
    import spark.implicits._

    val df = List("2021-10-05", "20211005", null).toDF("dateCol")

    it("should work as spark unix_timestamp function") {
      val dfString = List("2021-10-05 01:02:03", "20211005", null)
        .toDF("dateCol")

      dfString.testColumns("dateCol")(
        d => colString(d).unixTimestamp,
        d => f.unix_timestamp(f.col(d)),
        List(Some(1633395723L), None, None)
      )
    }

    it("should work as spark unix_timestamp(pattern) function") {
      df.testColumns2("dateCol", "yyyy-mm-dd")(
        (d, m) => colString(d).unixTimestamp(m.lit),
        (d, m) => f.unix_timestamp(f.col(d), m),
        List(Some(1609805400L), None, None)
      )
    }

    if (spark.version.take(3) > "3.0") {
      it("should fail if malformed format") {
        intercept[java.lang.IllegalArgumentException](
          df.select(colString("dateCol").unixTimestamp("yabcd".lit))
            .collect()
        )
      }
    }
  }

  describe("reverse doric function") {
    import spark.implicits._

    val df = List("hello world", "12345", null).toDF("col1")

    it("should work as spark reverse function") {
      df.testColumns("col1")(
        str => colString(str).reverse,
        str => f.reverse(f.col(str)),
        List("dlrow olleh", "54321", null).map(Option(_))
      )
    }
  }

  describe("conv doric function") {
    import spark.implicits._

    val df = List("100", "10", null).toDF("col1")

    it("should work as spark conv function") {
      df.testColumns3("col1", 2, 10)(
        (num, from, to) => colString(num).conv(from.lit, to.lit),
        (num, from, to) => f.conv(f.col(num), from, to),
        List(Some("4"), Some("2"), None)
      )
    }
  }

  describe("unHex doric function") {
    import spark.implicits._

    val df = List("5F", "-", null).toDF("col1")

    it("should work as spark unhex function") {
      df.testColumns("col1")(
        hexColName => colString(hexColName).unHex,
        hexColName => f.unhex(f.col(hexColName)),
        List(Some(Array[Byte](95)), None, None)
      )
    }
  }

  describe("schemaOfJson doric function") {
    import spark.implicits._

    val df = List("column not read").toDF("col1")

    it("should work as spark schema_of_json function") {
      val expected =
        if (spark.version < "3.1.0")
          List(Some("array<struct<col:bigint>>"))
        else if (spark.version >= "3.1.0" && spark.version < "3.3.0")
          List(Some("ARRAY<STRUCT<`col`: BIGINT>>"))
        else List(Some("ARRAY<STRUCT<col: BIGINT>>"))

      df.testColumns("[{'col':0}]")(
        c => c.lit.schemaOfJson(),
        c => f.schema_of_json(f.lit(c)),
        expected
      )
    }
  }

  describe("fromJson doric function") {
    import spark.implicits._

    val df =
      List("{\"a\": 1,\"b\": \"a\",\"date\": \"26/08/2015\"}").toDF("col1")

    it("should work as spark from_json(column) function") {
      df.testColumns2("col1", "a INTEGER, b STRING, date STRING")(
        (c, schema) => colString(c).fromJsonString(schema.lit),
        (c, schema) => f.from_json(f.col(c), f.lit(schema)),
        List(Some(Row(1, "a", "26/08/2015")))
      )
    }

    it("should work as spark from_json(column) function with options") {
      df.testColumns3(
        "col1",
        "a INTEGER, b STRING, date Timestamp",
        Map("timestampFormat" -> "dd/MM/yyyy")
      )(
        (
            c,
            schema,
            options
        ) => colString(c).fromJsonString(schema.lit, options),
        (c, schema, options) =>
          f.from_json(f.col(c), f.lit(schema), options.asJava),
        List(Some(Row(1, "a", Timestamp.valueOf("2015-08-26 00:00:00"))))
      )
    }

    it("should work as spark from_csv(structType) function") {
      df.testColumns2(
        "col1",
        StructType.fromDDL("a INTEGER, b STRING, date STRING")
      )(
        (c, schema) => colString(c).fromJsonStruct(schema),
        (c, schema) => f.from_json(f.col(c), schema, Map.empty[String, String]),
        List(Some(Row(1, "a", "26/08/2015")))
      )
    }

    it("should work as spark from_csv(structType) function with options") {
      df.testColumns3(
        "col1",
        StructType.fromDDL("a INTEGER, b STRING, date Timestamp"),
        Map("timestampFormat" -> "dd/MM/yyyy")
      )(
        (c, schema, options) => colString(c).fromJsonStruct(schema, options),
        (c, schema, options) => f.from_json(f.col(c), schema, options),
        List(Some(Row(1, "a", Timestamp.valueOf("2015-08-26 00:00:00"))))
      )
    }

    it("should work as spark from_csv(dataType) function") {
      df.testColumns2(
        "col1",
        DataType.fromDDL("a INTEGER, b STRING, date STRING")
      )(
        (c, schema) => colString(c).fromJsonDataType(schema),
        (c, schema) => f.from_json(f.col(c), schema, Map.empty[String, String]),
        List(Some(Row(1, "a", "26/08/2015")))
      )
    }

    it("should work as spark from_csv(dataType) function with options") {
      df.testColumns3(
        "col1",
        DataType.fromDDL("a INTEGER, b STRING, date Timestamp"),
        Map("timestampFormat" -> "dd/MM/yyyy")
      )(
        (c, schema, options) => colString(c).fromJsonDataType(schema, options),
        (c, schema, options) => f.from_json(f.col(c), schema, options),
        List(Some(Row(1, "a", Timestamp.valueOf("2015-08-26 00:00:00"))))
      )
    }
  }

  describe("getJsonObject doric function") {
    import spark.implicits._

    val df = List(
      "{\"a\": 1,\"b\": \"a\",\"date\": \"26/08/2015\"}",
      "{\"a\": 2,\"b\": \"test\",\"date\": \"26/08/2015\"}",
      "{\"a\": 3}"
    ).toDF("col1")

    it("should work as spark get_json_object function") {
      df.testColumns2("col1", "$.b")(
        (c, path) => colString(c).getJsonObject(path.lit),
        (c, path) => f.get_json_object(f.col(c), path),
        List(Some("a"), Some("test"), None)
      )
    }
  }

  describe("jsonTuple doric function") {
    import spark.implicits._

    val df = List(
      "{\"a\": 1,\"b\": \"a\",\"date\": \"26/08/2015\"}",
      "{\"a\": 2,\"b\": \"test\",\"date\": \"26/08/2015\"}",
      "{\"a\": 3}"
    ).toDF("col1")

    it("should work as spark json_tuple function") {
      val rows = df
        .select(colString("col1").jsonTuple("a".lit, "b".lit))
        .as[(String, String)]
        .collect()
        .toList
      rows shouldBe df
        .select(f.json_tuple(f.col("col1"), "a", "b"))
        .as[(String, String)]
        .collect()
        .toList
      rows.map(Option(_)) shouldBe List(
        Some(("1", "a")),
        Some(("2", "test")),
        Some(("3", null))
      )
    }

    it("should work with columns") {
      val df2 = List(
        ("{\"a\": 1,\"b\": \"a\",\"date\": \"26/08/2015\"}", "a"),
        ("{\"a\": 2,\"b\": \"test\",\"date\": \"26/08/2015\"}", "b"),
        ("{\"a\": 3}", "j")
      ).toDF("col1", "col2")
      val rows = df2
        .select(colString("col1").jsonTuple(colString("col2")))
        .as[String]
        .collect()
        .toList
      rows.map(Option(_)) shouldBe
        List(Some("1"), Some("test"), None)
    }
  }

  describe("comparison operators") {
    import spark.implicits._

    val df =
      List(("abc", "xyz"), ("abc", "abc"), ("xyz", "abc"), ("xyz", "xyz"))
        .toDF("col1", "col2")

    it("> should work as spark > function") {
      df.testColumns2("col1", "col2")(
        (col1, col2) => colString(col1) > colString(col2),
        (col1, col2) => f.col(col1) > f.col(col2),
        List(false, false, true, false).map(Option(_))
      )
    }

    it(">= should work as spark >= function") {
      df.testColumns2("col1", "col2")(
        (col1, col2) => colString(col1) >= colString(col2),
        (col1, col2) => f.col(col1) >= f.col(col2),
        List(false, true, true, true).map(Option(_))
      )
    }

    it("< should work as spark < function") {
      df.testColumns2("col1", "col2")(
        (col1, col2) => colString(col1) < colString(col2),
        (col1, col2) => f.col(col1) < f.col(col2),
        List(true, false, false, false).map(Option(_))
      )
    }

    it("<= should work as spark <= function") {
      df.testColumns2("col1", "col2")(
        (col1, col2) => colString(col1) <= colString(col2),
        (col1, col2) => f.col(col1) <= f.col(col2),
        List(true, true, false, true).map(Option(_))
      )
    }
  }

}
