package doric
package sem

class AggregationOpsSpec extends DoricTestElements {

  import spark.implicits._
  describe("Aggregate") {
    val str  = "str"
    val str2 = "str2"
    val num2 = "num2"
    val num  = "num"
    val sum1 = "sum"
    val conc = "conc"
    val df =
      List((1, "5", 1), (3, "5", 2), (2, "3", 3))
        .toDF(num, str, num2)

    it("can use original spark aggregateFunctions") {
      df.groupBy(str)
        .agg(colInt(num).pipe(sum(_)) as sum1)
        .validateColumnType(colLong(sum1))

      assertThrows[DoricMultiError] {
        df.groupBy(str)
          .agg(colLong(num).pipe(sum(_)) as sum1)
      }
    }

    it("groupBy") {
      df.groupBy(concat(col(str), col(str)) as conc)
        .agg(col[Int](num).pipe(sum(_)) as sum1)
        .validateColumnType(colString(conc))
        .validateColumnType(colLong(sum1))

      assertThrows[DoricMultiError] {
        df.groupBy(col[String](str2))
          .agg(col[Int](num).pipe(sum(_)) as sum1)
      }
    }

    it("cube") {
      df.cube(concat(col(str), col(str)) as conc)
        .agg(col[Int](num).pipe(sum(_)) as sum1)
        .validateColumnType(colString(conc))
        .validateColumnType(colLong(sum1))

      assertThrows[DoricMultiError] {
        df.cube(col[String](str2))
          .agg(col[Int](num).pipe(sum(_)) as sum1)
      }
    }

    it("rollup") {
      df.rollup(concat(col(str), col(str)) as conc)
        .agg(col[Int](num).pipe(sum(_)) as sum1)
        .validateColumnType(colString(conc))
        .validateColumnType(colLong(sum1))

      assertThrows[DoricMultiError] {
        df.rollup(col[String](str2))
          .agg(col[Int](num).pipe(sum(_)) as sum1)
      }
    }

    it("pivot") {
      val value1 = "1_first"
      val str1   = "4_" + sum1
      val str3   = "1_" + sum1
      val firstC = "first"
      val value2 = "4_first"
      df.groupBy(concat(str.cname, str.cname) as conc)
        .pivot(colInt(num2))(List(1, 4))
        .agg(
          col[Int](num).pipe(sum(_)) as sum1,
          col[Int](num).pipe(first(_)) as firstC
        )
        .validateColumnType(colString(conc))
        .validateColumnType(colLong(str3))
        .validateColumnType(colLong(str1))
        .validateColumnType(colInt(value1))
        .validateColumnType(colInt(value2))

      assertThrows[DoricMultiError] {
        df.groupBy(concat(col(str), col(str)) as conc)
          .pivot(colString(num2))(List("1", "4"))
          .agg(col[Int](num).pipe(sum(_)) as sum1)
      }
    }
  }
}
