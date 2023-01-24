package doric.sqlExpressions

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.DataType

case class CustomAgg(
    child: Expression,
    initialValue: Expression,
    update: (Expression, Expression) => Expression,
    merge: (Expression, Expression) => Expression,
    evaluate: Expression => Expression
) extends DeclarativeAggregate
    with UnaryLike[Expression] {

  private final val accum =
    AttributeReference("accum", initialValue.dataType)()

  override val initialValues: Seq[Expression]     = Seq(initialValue)
  override val updateExpressions: Seq[Expression] = Seq(update(accum, child))
  override val mergeExpressions: Seq[Expression] = Seq(
    merge(accum.left, accum.right)
  )
  override val evaluateExpression: Expression = evaluate(accum)

  override protected def withNewChildInternal(
      newChild: Expression
  ): Expression = copy(child = newChild)

  override lazy val nullable: Boolean = child.nullable

  override lazy val dataType: DataType = evaluate(accum).dataType

  override def aggBufferAttributes: Seq[AttributeReference] = Seq(accum)
}
