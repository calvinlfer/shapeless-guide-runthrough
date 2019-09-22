import shapeless.ops.hlist.{ Init, Last }

object Ch6 extends App {

  /**
   * Lemma pattern
   * If we find a particular sequence of ops useful, we can package them up and re-provide them as another ops type
   * class. This is an example of the “lemma” pattern..
   */
  import shapeless._
  trait Penultimate[L] {
    type Out
    def apply(l: L): Out
  }

  object Penultimate {
    type Aux[I, O] = Penultimate[I] { type Out = O }

    // Summoner
    // NOTE: Penultimate.Aux[L, p.Out] === PenUltimate[L] { type Out = p.Out }
    // meaning that Penultimate[L] == Penultimate.Aux[L, O] where we provide extra type information in the case of Aux
    // We use Aux to ensure type members are visible on the summoned instances
    // See https://books.underscore.io/shapeless-guide/shapeless-guide.html#sec:type-level-programming:depfun
    // Using the Aux pattern is to prevent erasing type members on summoned instances causing implicitly to not work
    // if you want to do this properly, define a summoner like below or use `the` from Shapeless
    def apply[L](implicit p: Penultimate[L]): Penultimate.Aux[L, p.Out] = p

    // only provide an implementation for HLists with 2 or more elements
    // Note that Final does not have a type constraint because we are returning one element
    // (i.e. Penultimate[
    implicit def hlistInstance[One <: HList, Two <: HList, Final](
      implicit
      init: Init.Aux[One, Two], // apply init on One to obtain Two
      last: Last.Aux[Two, Final] // apply last on Two to obtain Final
    ): Penultimate.Aux[One, Final] = new Penultimate[One] {
      type Out = Final

      // this is the only valid construction to make the types match
      override def apply(l: One): Final = last(init(l))
    }
  }

  type Example = Int :: String :: Double :: String :: HNil
  val example: Example = 1 :: "two" :: 3.0 :: "Four" :: HNil
  println(Penultimate[Example].apply(example))                             // 3.0
  println(Penultimate[String :: Double :: HNil].apply("1" :: 2.0 :: HNil)) // "1"
}
