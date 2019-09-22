import cats.Monoid
import shapeless.ops.hlist._
import shapeless.labelled.{ field, FieldType }
import cats.instances.all._

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

  def createMonoid[A](zero: A)(add: (A, A) => A): Monoid[A] =
    new Monoid[A] {
      override def empty: A = zero

      override def combine(x: A, y: A): A = add(x, y)
    }

  implicit val hnilMonoid: Monoid[HNil] =
    createMonoid[HNil](HNil)((_, _) => HNil)

  implicit def emptyHList[K <: Symbol, H, T <: HList](
    implicit
    hMonoid: Lazy[Monoid[H]],
    tMonoid: Monoid[T]
  ): Monoid[FieldType[K, H] :: T] =
    createMonoid(field[K](hMonoid.value.empty) :: tMonoid.empty) { (x, y) =>
      field[K](hMonoid.value.combine(x.head, y.head)) ::
        tMonoid.combine(x.tail, y.tail)
    }

  trait Migration[A, B] {
    def apply(a: A): B
  }

  implicit class MigrationOps[A](a: A) {
    def migrateTo[B](implicit m: Migration[A, B]): B = m(a)
  }

  implicit def genericMigration[
    A, B,
    ARep <: HList, BRep <: HList,
    Common <: HList, Added <: HList, Unaligned <: HList
  ](
    implicit
    aGen: LabelledGeneric.Aux[A, ARep],                 // 1. proof that we can convert A to/from generic rep (ARep)
    bGen: LabelledGeneric.Aux[B, BRep],                 // 2. proof that we can convert B to/from generic rep (BRep)
    intersection: Intersection.Aux[ARep, BRep, Common], // 3. what fields are the same between ARep and BRep, the output is Common
    difference: Diff.Aux[BRep, Common, Added],          // 4. what fields are different between BRep and Common, the output is Added
    monoid: Monoid[Added],                              // 5. we need default values for Added since B does not have them but A does
    prepend: Prepend.Aux[Added, Common, Unaligned],     // 6. Add on the extra fields to the Common data type, the output is Unaligned
    align: Align[Unaligned, BRep]                       // 7. Align fields so we can get it into B's generic representation
  ): Migration[A, B] = (a: A) => {
    val aRep        = aGen.to(a)
    val common      = intersection.apply(aRep)
    val unaligned   = prepend.apply(monoid.empty, common)
    val alignedBRep = align.apply(unaligned)
    val b           = bGen.from(alignedBRep)
    b
  }

  case class IceCreamV1(name: String, numCherries: Int, inCone: Boolean)

  // Remove fields
  case class IceCreamV2a(name: String, inCone: Boolean)

  // Reorder fields:
  case class IceCreamV2b(name: String, inCone: Boolean, numCherries: Int)

  // Insert fields (provided we can determine a default value):
  case class IceCreamV2c(name: String, inCone: Boolean, numCherries: Int, numWaffles: Int)

  println {
    IceCreamV1("Sundae", 1, false).migrateTo[IceCreamV2a]
  }

  // Reorder fields

  println {
    IceCreamV1("Sundae", 1, true).migrateTo[IceCreamV2b]
  }

  // Add new fields
  println {
    IceCreamV1("Sundae", 1, true).migrateTo[IceCreamV2c]
  }

}
