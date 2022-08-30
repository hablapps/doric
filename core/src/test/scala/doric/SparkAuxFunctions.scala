package doric

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{LambdaFunction, UnresolvedNamedLambdaVariable}

import java.util.concurrent.atomic.AtomicInteger

object SparkAuxFunctions {

  /**
    * Auxiliary object in order to access freshVarName method for it is not available in old spark versions
    */
  private object UnresolvedNamedLambdaVariable_Aux {

    // Counter to ensure lambda variable names are unique
    private val nextVarNameId = new AtomicInteger(0)

    def freshVarName(name: String): String = {
      s"${name}_${nextVarNameId.getAndIncrement()}"
    }
  }

  private lazy val getVariable: String => UnresolvedNamedLambdaVariable =
    name =>
      UnresolvedNamedLambdaVariable(
        Seq(UnresolvedNamedLambdaVariable_Aux.freshVarName(name))
      )

  def createLambda(f: Column => Column): LambdaFunction = {
    val x        = getVariable("x")
    val function = f(new Column(x)).expr
    LambdaFunction(function, Seq(x))
  }

  def createLambda(f: (Column, Column) => Column): LambdaFunction = {
    val x        = getVariable("x")
    val y        = getVariable("y")
    val function = f(new Column(x), new Column(y)).expr
    LambdaFunction(function, Seq(x, y))
  }

  def createLambda(f: (Column, Column, Column) => Column): LambdaFunction = {
    val x        = getVariable("x")
    val y        = getVariable("y")
    val z        = getVariable("z")
    val function = f(new Column(x), new Column(y), new Column(z)).expr
    LambdaFunction(function, Seq(x, y, z))
  }

}
