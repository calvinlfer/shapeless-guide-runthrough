

object Ch2 extends App {
  import shapeless.{HList, ::, HNil}

  val product: String :: Int :: Boolean :: HNil =
    "Sunday" :: 1 :: false :: HNil

  val newProduct: Int :: String :: Int :: Boolean :: HNil = 42 :: product

  // 2.2.1 Switching representations using Generic
  import shapeless.Generic
  import shapeless.Generic.Aux

  case class IceCream(name: String, numCherries: Int, inCone: Boolean)
  val iceCreamGenRep: Aux[IceCream, String :: Int :: Boolean :: HNil] = Generic[IceCream]

  val sundae: IceCream = IceCream("Sundae", 1, false)
  val sundaeRepr: String :: Int :: Boolean :: HNil = iceCreamGenRep.to(sundae)
  val backSundae: IceCream = iceCreamGenRep.from(sundaeRepr)

  case class Employee(name: String, number: Int, manager: Boolean)
  // Create an employee from an ice cream:
  val employee: Employee = Generic[Employee].from(Generic[IceCream].to(sundae))

  import shapeless.{Coproduct, :+:, CNil, Inl, Inr}

  case class Red()
  case class Amber()
  case class Green()

  type Light = Red :+: Amber :+: Green :+: CNil
  val red: Light = Inl(Red())
  val green: Light = Inr(Inr(Inl(Green())))
  val amber: Light = Inr(Inl(Amber()))

  sealed trait Shape
  final case class Rectangle(width: Double, height: Double) extends Shape
  final case class Circle(radius: Double) extends Shape

  val genShape = Generic[Shape]
  val genRec = genShape.to(Rectangle(3.0, 4.0))
  println(genRec)
  val genCir = genShape.to(Circle(1.0))
  println(genCir)
}
