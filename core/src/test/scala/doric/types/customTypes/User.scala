package doric
package types
package customTypes

case class User(name: String, surname: String)

object User {
  implicit val userst: SparkType[User] {
    type OriginalSparkType = String
  } =
    SparkType[String].customType[User](x => {
      val name :: surname :: Nil = x.split("#").toList
      User(name, surname)
    })

  implicit val userlst: LiteralSparkType[User] {
    type OriginalSparkType = String
  } =
    LiteralSparkType.customType[String, User](x => s"${x.name}#${x.surname}")
}
