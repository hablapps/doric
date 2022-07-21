package doric

import doric.matchers.CustomMatchers
import org.apache.spark.sql.internal.SQLConf.buildConf
import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpecLike
import org.scalatest.matchers.should.Matchers

private[doric] trait DoricTestElements
    extends AnyFunSpecLike
    with SparkSessionTestWrapper
    with TypedColumnTest
    with EitherValues
    with Matchers
    with CustomMatchers

object DoricTestElements extends DoricTestElementsCompanion_Specific
