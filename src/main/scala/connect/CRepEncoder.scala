package connect

import shapeless.{ :+:, ::, CNil, Coproduct, HList, HNil, Inl, Inr, LabelledGeneric, Lazy, Witness }
import shapeless.labelled.FieldType
import java.math.{ BigDecimal => JBigDecimal }

trait CRepEncoder[A] {
  def encode(a: A): CRep
}

object CRepEncoder extends LowPriorityImplicits {
  // This is solely used for encoding case classes
  private[connect] trait CStructEncoder[A] extends CRepEncoder[A] {
    def encode(a: A): CStruct
  }

  // summoner
  def apply[A](implicit C: CRepEncoder[A]): CRepEncoder[A] =
    C
  def convert[A](f: A => CRep): CRepEncoder[A] = (a: A) => f(a)

  def convertS[A](f: A => CStruct): CStructEncoder[A] =
    (a: A) => f(a)

  implicit val bigDecimalEncoder: CRepEncoder[BigDecimal]   = convert(b => CBigDecimal(b.bigDecimal))
  implicit val jBigDecimalEncoder: CRepEncoder[JBigDecimal] = convert(CBigDecimal)
  implicit val byteEncoder: CRepEncoder[Byte]               = convert(CInt8)
  implicit val bytesEncoder: CRepEncoder[Array[Byte]]       = convert(CBytes)
  implicit val shortEncoder: CRepEncoder[Short]             = convert(CInt16)
  implicit val intEncoder: CRepEncoder[Int]                 = convert(CInt32)
  implicit val longEncoder: CRepEncoder[Long]               = convert(CInt64)
  implicit val charEncoder: CRepEncoder[Char]               = convert(CCh)
  implicit val floatEncoder: CRepEncoder[Float]             = convert(CFloat32)
  implicit val doubleEncoder: CRepEncoder[Double]           = convert(CFloat64)
  implicit val stringEncoder: CRepEncoder[String]           = convert(CStr)
  implicit def optionEncoder[A: CRepEncoder]: CRepEncoder[OptionWithDefault[A]] =
    convert { od: OptionWithDefault[A] =>
      COpD(
        OptionWithDefault(od.o.map(a => CRepEncoder[A].encode(a)), CRepEncoder[A].encode(od.default))
      )
    }

  implicit val hnilEncoder: CStructEncoder[HNil] = convertS(_ => CStruct(Nil))

  implicit def hlistEncoder[K <: Symbol, H, T <: HList](
    implicit
    witness: Witness.Aux[K],
    hEncoder: Lazy[CRepEncoder[H]],
    tEncoder: CStructEncoder[T]
  ): CStructEncoder[FieldType[K, H] :: T] = convertS {
    case h :: t =>
      val fieldName = witness.value.name
      val head      = (fieldName, hEncoder.value.encode(h))
      val tail      = tEncoder.encode(t)
      CStruct(head :: tail.underlying)
  }

  implicit val cnilEncoder: CStructEncoder[CNil] =
    convertS(_ => throw new Exception("This cannot happen as CNil is equivalent to Nothing and has no type inhabitants"))

  // NOTE: I'm expecting a sealed trait whose subtypes are case classes so they will only produce CStructs
  // So I use hEncoder: Lazy[ConnectSchemaStructEncoder[H]], instead of ConnectSchemaEncoder like we did in HList
  // here a Witness on K refers to the name of the sealed trait subtype
  implicit def coproductEncoder[K <: Symbol, H, T <: Coproduct](
    implicit
    witness: Witness.Aux[K],
    hEncoder: Lazy[CStructEncoder[H]],
    tEncoder: CStructEncoder[T]
  ): CStructEncoder[FieldType[K, H] :+: T] = convertS {
    case Inl(head) =>
      val typeName = witness.value.name
      val existing = hEncoder.value.encode(head).underlying
      val newInfo  = "type" -> CStr(typeName)
      CStruct(newInfo :: existing)

    case Inr(tail) =>
      tEncoder.encode(tail)
  }
}

trait LowPriorityImplicits {
  import connect.CRepEncoder.{ convertS, CStructEncoder }
  // Given an encoder for the generic representation R, and a conversion from a specific representation A (case class)
  // to a generic representation R (HList) we can obtain an encoder for the specific representation (A)
  implicit def genericEncoder[A, R](
    implicit
    gen: LabelledGeneric.Aux[A, R],
    enc: Lazy[CStructEncoder[R]]
  ): CStructEncoder[A] =
    convertS(a => enc.value.encode(gen.to(a)))
}
