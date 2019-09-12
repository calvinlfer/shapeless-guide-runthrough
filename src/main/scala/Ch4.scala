import shapeless._

object Ch4 extends App {

  def getRepr[A](value: A)(implicit gen: Generic[A]): gen.Repr = gen.to(value)

  trait Second[L <: HList] {
    type Out
    def apply(value: L): Out
  }

  object Second {
    type Aux[L <: HList, O] = Second[L] { type Out = O }

    def apply[L <: HList](implicit inst: Second[L]): Aux[L, inst.Out] =
      inst
  }

  import Second._

  implicit def hlistSecond[A, B, Rest <: HList]: Aux[A :: B :: Rest, B] =
    new Second[A :: B :: Rest] {
      override type Out = B

      override def apply(value: A :: B :: Rest): B = value.tail.head
    }
}
