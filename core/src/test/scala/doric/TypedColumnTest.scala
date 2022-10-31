package doric

import scala.reflect._
import scala.reflect.runtime.universe.TypeTag

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import doric.Equalities._
import doric.implicitConversions.stringCname
import doric.sem.Location
import doric.types.{Casting, LiteralSparkType, SparkType}

import org.apache.spark.sql.catalyst.ScalaReflection
import org.scalactic._
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Column, DataFrame, Encoder, RelationalGroupedDataset, Row, SparkSession, functions => f}
import org.apache.spark.sql.types._

trait TypedColumnTest extends Matchers with DatasetComparer {

  private lazy val doricCol = "dcol"
  private lazy val sparkCol = "scol"

  def testDataType[T: TypeTag: SparkType](implicit
      spark: SparkSession,
      pos: source.Position
  ): Unit =
    SparkType[T].dataType shouldBe ScalaReflection.schemaFor[T].dataType

  def testDataTypeForEncoder[T: TypeTag: SparkType: Encoder](implicit
      spark: SparkSession,
      pos: source.Position
  ): Unit =
    noException shouldBe thrownBy {
      spark
        .emptyDataset[T]
        .toDF()
        .select(col[T]("value"))
    }

  def testLitDataType[T: TypeTag: LiteralSparkType: SparkType](
      value: T
  )(implicit spark: SparkSession, pos: source.Position): Unit =
    spark.emptyDataFrame
      .select(value.lit)
      .schema
      .fields
      .head
      .dataType shouldBe ScalaReflection.schemaFor[T].dataType

  def deserializeSparkType[T: TypeTag: SparkType: Equality](
      data: T
  )(implicit spark: SparkSession, pos: source.Position): Unit =
    spark
      .createDataFrame(Seq((data, 0)))
      .collectCols[T](col[T]("_1"))
      .head should ===(data)

  def deserializeSparkType(data: Row)(implicit
      spark: SparkSession,
      pos: source.Position,
      rowST: SparkType[Row],
      rowEq: Equality[Row]
  ): Unit = {
    val tuple2Row: Row = new GenericRowWithSchema(
      Array(data, 1),
      StructType(
        List(StructField("_1", data.schema), StructField("_2", IntegerType))
      )
    )
    spark
      .createDataFrame(
        spark.sparkContext.makeRDD(Seq(tuple2Row)),
        tuple2Row.schema
      )
      .collectCols[Row](col[Row]("_1"))
      .head should ===(data)
  }

  def serializeSparkType[T: LiteralSparkType: SparkType: Equality](
      data: T
  )(implicit spark: SparkSession, loc: Location, pos: source.Position): Unit =
    spark
      .range(1)
      .toDF()
      .select(data.lit as "value")
      .collectCols[T](col[T]("value"))
      .head should ===(data)

  /**
    * Compare two columns (doric & spark).
    * If `expected` is defined is also compared
    *
    * @param df
    * Spark dataFrame
    * @param expected
    * list of values
    * @tparam T
    * Comparing column type
    */
  def compareDifferences[T: SparkType: TypeTag: Equality](
      df: DataFrame,
      expected: List[Option[T]],
      fixFunction: Option[T => T] = None
  ): Unit = {

    val fixedExpectedData = fixFunction match {
      case Some(f) => expected.map(_.map(f))
      case None    => expected
    }

    val eqCond: BooleanColumn = SparkType[T].dataType match {
      case _: MapType =>
        val compare: (Column => Column) => BooleanColumn = sparkFun =>
          {
            sparkFun(f.col(doricCol)) === sparkFun(f.col(sparkCol))
          }.asDoric[Boolean]

        compare(f.map_keys) and compare(f.map_values)
      case _ => col[T](doricCol) === col(sparkCol)
    }

    val bothNull = col(doricCol).isNull and col(sparkCol).isNull

    val equalsColumn = "equals"
    val result = df
      .withColumn(equalsColumn, eqCond or bothNull)
      .na
      .fill(Map(equalsColumn -> false))

    val (doricColumns, sparkColumns, boolResColumns) = result
      .collectCols(
        col[Option[T]](doricCol),
        col[Option[T]](sparkCol),
        colBoolean(equalsColumn)
      )
      .unzip3

    assert(
      boolResColumns.reduce(_ && _),
      s"\nDoric function & Spark function return different values\n" +
        s"Doric   : $doricColumns\n" +
        s"Spark   : $sparkColumns}" +
        s"${if (fixedExpectedData.nonEmpty) s"\nExpected: $fixedExpectedData"}"
    )

    if (fixedExpectedData.nonEmpty) {
      assert(doricColumns.map {
        case Some(x: java.lang.Double) if x.isNaN => None
        case x                                    => x
      } === fixedExpectedData)
    }
  }

  implicit class ValidateColumnGroupType(gr: RelationalGroupedDataset) {

    /**
      * Tests doric and spark aggregation functions
      *
      * @param aggDoricCol
      *   Doric aggregation column
      * @param aggSparkCol
      *   Spark aggregation column
      * @param expected
      *   list of values
      * @tparam T
      *   Comparing column type
      */
    def testGrouped[T: SparkType: TypeTag: Equality](
        aggDoricCol: DoricColumn[T],
        aggSparkCol: Column,
        expected: List[Option[T]] = List.empty
    ): Unit = {

      val aggColName = "aggCol"
      val doricDF    = gr.agg(aggDoricCol.as(aggColName))
      val sparkDF    = gr.agg(aggSparkCol.as(aggColName))

      assertSmallDatasetEquality(doricDF, sparkDF)

      if (expected.nonEmpty) {
        implicit val enc: Encoder[Option[T]] =
          doricDF.sparkSession.implicits.newProductEncoder[Option[T]]
        val rows = doricDF.select(aggColName).as[Option[T]].collect().toList
        rows should contain theSameElementsAs expected
      }
    }
  }

  implicit class ValidateColumnType(df: DataFrame) {

    /**
      * Tests doric and spark aggregation functions
      *
      * @param keyCol
      *   CName to group by
      * @param aggDoricCol
      *   Doric aggregation column
      * @param aggSparkCol
      *   Spark aggregation column
      * @param expected
      *   list of values
      * @tparam T
      *   Comparing column type
      */
    def testAggregation[T: SparkType: TypeTag: Equality](
        keyCol: CName,
        aggDoricCol: DoricColumn[T],
        aggSparkCol: Column,
        expected: List[Option[T]] = List.empty
    ): Unit = {
      val grouped = df.groupByCName(keyCol)

      val result = grouped
        .agg(
          aggDoricCol.as(doricCol),
          aggSparkCol.asDoric[T].as(sparkCol)
        )
        .selectCName(doricCol, sparkCol)
        .orderBy(keyCol.value)

      compareDifferences(result, expected)
    }

    /**
      * Tests doric & spark functions without parameters or columns
      *
      * @param dcolumn
      *   doric column
      * @param scolumn
      *   spark column
      * @param expected
      *   list of values
      * @tparam I1
      *   literal or column type
      * @tparam T
      *   Comparing column type
      */
    def testColumn[I1, T: SparkType: TypeTag: Equality](
        dcolumn: DoricColumn[T],
        scolumn: Column,
        expected: List[Option[T]] = List.empty
    ): Unit = {

      val result = df.select(
        dcolumn.as(doricCol),
        scolumn.asDoric[T].as(sparkCol)
      )

      compareDifferences(result, expected)
    }

    /**
      * Tests doric & spark function from a column or literal
      *
      * @param column1
      *   literal or column name
      * @param dcolumn
      *   function to get doric column
      * @param scolumn
      *   function to get spark column
      * @param expected
      *   list of values
      * @tparam I1
      *   literal or column type
      * @tparam T
      *   Comparing column type
      */
    def testColumns[I1, T: SparkType: TypeTag: Equality](
        column1: I1
    )(
        dcolumn: I1 => DoricColumn[T],
        scolumn: I1 => Column,
        expected: List[Option[T]] = List.empty,
        fixFunction: Option[T => T] = None
    ): Unit = {

      val result = df.select(
        dcolumn(column1).as(doricCol),
        scolumn(column1).asDoric[T].as(sparkCol)
      )

      compareDifferences(result, expected, fixFunction)
    }

    /**
      * Tests doric & spark function from two columns or literals
      *
      * @param column1
      *   literal or column name
      * @param column2
      *   literal or column name
      * @param dcolumn
      *   function to get doric column
      * @param scolumn
      *   function to get spark column
      * @param expected
      *   list of values
      * @tparam I1
      *   literal or column type
      * @tparam I2
      *   literal or column type
      * @tparam T
      *   Comparing column type
      */
    def testColumns2[I1, I2, T: SparkType: TypeTag: Equality](
        column1: I1,
        column2: I2
    )(
        dcolumn: (I1, I2) => DoricColumn[T],
        scolumn: (I1, I2) => Column,
        expected: List[Option[T]] = List.empty,
        fixFunction: Option[T => T] = None
    ): Unit = {

      val result = df.select(
        dcolumn(column1, column2).as(doricCol),
        scolumn(column1, column2).asDoric[T].as(sparkCol)
      )

      compareDifferences(result, expected, fixFunction)
    }

    /**
      * Tests doric & spark function from three columns or literals
      *
      * @param column1
      *   literal or column name
      * @param column2
      *   literal or column name
      * @param column3
      *   literal or column name
      * @param dcolumn
      *   function to get doric column
      * @param scolumn
      *   function to get spark column
      * @param expected
      *   list of values
      * @tparam I1
      *   literal or column type
      * @tparam I2
      *   literal or column type
      * @tparam I3
      *   literal or column type
      * @tparam T
      *   Comparing column type
      */
    def testColumns3[I1, I2, I3, T: SparkType: TypeTag: Equality](
        column1: I1,
        column2: I2,
        column3: I3
    )(
        dcolumn: (I1, I2, I3) => DoricColumn[T],
        scolumn: (I1, I2, I3) => Column,
        expected: List[Option[T]] = List.empty
    ): Unit = {

      val result = df.select(
        dcolumn(column1, column2, column3).as(doricCol),
        scolumn(column1, column2, column3).asDoric[T].as(sparkCol)
      )

      compareDifferences(result, expected)
    }

    /**
      * Tests doric & spark function from four columns or literals
      *
      * @param column1
      *   literal or column name
      * @param column2
      *   literal or column name
      * @param column3
      *   literal or column name
      * @param column4
      *   literal or column name
      * @param dcolumn
      *   function to get doric column
      * @param scolumn
      *   function to get spark column
      * @param expected
      *   list of values
      * @tparam I1
      *   literal or column type
      * @tparam I2
      *   literal or column type
      * @tparam I3
      *   literal or column type
      * @tparam I4
      *   literal or column type
      * @tparam T
      *   Comparing column type
      */
    def testColumns4[I1, I2, I3, I4, T: SparkType: TypeTag: Equality](
        column1: I1,
        column2: I2,
        column3: I3,
        column4: I4
    )(
        dcolumn: (I1, I2, I3, I4) => DoricColumn[T],
        scolumn: (I1, I2, I3, I4) => Column,
        expected: List[Option[T]] = List.empty
    ): Unit = {

      val result = df.select(
        dcolumn(column1, column2, column3, column4).as(doricCol),
        scolumn(column1, column2, column3, column4).asDoric[T].as(sparkCol)
      )

      compareDifferences(result, expected)
    }

    def testColumnsN[T: SparkType: TypeTag: Equality](
        struct: StructType
    )(
        dcolumn: Seq[DoricColumn[_]] => DoricColumn[T],
        scolumn: Seq[Column] => Column,
        expected: List[Option[T]] = List.empty
    ): Unit = {

      val doricColumns: Seq[DoricColumn[_]] = struct.map {
        case StructField(name, dataType, _, _) =>
          dataType match {
            case NullType      => colNull(name)
            case StringType    => colString(name)
            case IntegerType   => colInt(name)
            case LongType      => colLong(name)
            case DoubleType    => colDouble(name)
            case BooleanType   => colBoolean(name)
            case DateType      => colDate(name)
            case TimestampType => colTimestamp(name)
            // TODO issue [[https://github.com/hablapps/doric/issues/149 #149]]
//          case ArrayType => colArray(name.cname)
//          case StructType => colStruct(name.cname)
//          case MapType => colMap(name.cname)
          }
      }

      val result = df.select(
        dcolumn(doricColumns).as(doricCol),
        scolumn(struct.map(x => f.col(x.name))).asDoric[T].as(sparkCol)
      )

      compareDifferences(result, expected)
    }

    def validateColumnType[T: SparkType](
        column: DoricColumn[T],
        show: Boolean = false
    ): DataFrame = {
      val colName          = "result"
      val df2              = df.withColumn(colName, column)
      val providedDatatype = df2(colName).expr.dataType
      assert(
        SparkType[T].isEqual(providedDatatype),
        s"the type of the column '$column' is not ${SparkType[T].dataType} is $providedDatatype"
      )
      if (show) {
        df2.show(false)
        df2
      } else {
        df2.foreach(_ =>
          ()
        ) // force a spark execution to check if in spark runtime the job fails
        df2
      }
    }
  }

  implicit class TestColumn[T: ClassTag: SparkType](tcolumn: DoricColumn[T]) {

    type Cast[To] = Casting[T, To]

    /**
      * Checks that the actual state of the column still has the same type of
      * the representation.
      *
      * @return
      *   the provided column
      */
    def withTypeChecked: DoricColumn[T] = {
      withTypeChecked(SparkType[T].dataType)
    }

    /**
      * Checks that the actual state of the column still has the same type of
      * the representation.
      *
      * @param expectedType
      *   the spark datatype expected in this moment
      * @return
      *   the provided column
      */
    def withTypeChecked(expectedType: DataType): DoricColumn[T] = {
      tcolumn.elem
        .map(c => {
          val columnType: DataType = c.expr.dataType
          assert(
            columnType == expectedType,
            s"the column expression type is $columnType but the wrapper " +
              s"${classTag[T].runtimeClass.getSimpleName} if of type $expectedType "
          )
          c
        })
        .toDC
    }

    /**
      * Cast and checks that the type is correct
      *
      * @param expectedType
      *   the spark datatype expected in this moment
      * @return
      *   the provided column casted to the type if
      */
    def testCastingTo[To: Cast: SparkType: ClassTag](
        expectedType: DataType
    ): DoricColumn[To] = {
      SparkType[To].dataType
      Casting[T, To].cast(tcolumn)
      tcolumn.cast[To].withTypeChecked.withTypeChecked(expectedType)
    }
  }

}
