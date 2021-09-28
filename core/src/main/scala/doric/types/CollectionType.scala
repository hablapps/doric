package doric.types

trait CollectionType[T[_]]

object CollectionType {
  implicit val arrayCollectionType: CollectionType[Array] =
    new CollectionType[Array] {}
  implicit val listCollectionType: CollectionType[List] =
    new CollectionType[List] {}
}
