package doric

package object sem {

  private[sem] implicit class ErrorThrower[T](
      element: DoricValidated[T]
  ) {

    private[sem] def returnOrThrow(functionType: String): T =
      element.fold(er => throw DoricMultiError(functionType, er), identity)
  }
}
