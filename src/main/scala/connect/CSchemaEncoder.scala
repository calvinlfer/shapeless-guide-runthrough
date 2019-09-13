package connect

import shapeless.{ :+:, ::, CNil, Coproduct, HList, HNil, Inl, Inr, LabelledGeneric, Lazy, Witness }
import shapeless.labelled.FieldType
import java.math.{ BigDecimal => JBigDecimal }

import connect.CSchemaEncoder.{ convertS, CStructEncoder }

trait CSchemaEncoder[A] {
  def encode(a: A): CSchema
}

object CSchemaEncoder extends LowPriorityImplicits {
  // This is solely used for encoding case classes
  private[connect] trait CStructEncoder[A] extends CSchemaEncoder[A] {
    def encode(a: A): CStruct
  }

  // summoner
  def apply[A](implicit C: CSchemaEncoder[A]): CSchemaEncoder[A] =
    C
  def convert[A](f: A => CSchema): CSchemaEncoder[A] = (a: A) => f(a)

  def convertS[A](f: A => CStruct): CStructEncoder[A] =
    (a: A) => f(a)

  implicit def bigDecimalEncoder(scale: Int): CSchemaEncoder[BigDecimal]   = convert(b => CBigDecimal(scale))
  implicit def jBigDecimalEncoder(scale: Int): CSchemaEncoder[JBigDecimal] = convert(b => CBigDecimal(scale))
  implicit val byteEncoder: CSchemaEncoder[Byte]                           = convert(_ => CInt8)
  implicit val bytesEncoder: CSchemaEncoder[Array[Byte]]                   = convert(_ => CBytes)
  implicit val shortEncoder: CSchemaEncoder[Short]                         = convert(_ => CInt16)
  implicit val intEncoder: CSchemaEncoder[Int]                             = convert(_ => CInt32)
  implicit val longEncoder: CSchemaEncoder[Long]                           = convert(_ => CInt64)
  implicit val charEncoder: CSchemaEncoder[Char]                           = convert(_ => CCh)
  implicit val floatEncoder: CSchemaEncoder[Float]                         = convert(_ => CFloat32)
  implicit val doubleEncoder: CSchemaEncoder[Double]                       = convert(_ => CFloat64)
  implicit val stringEncoder: CSchemaEncoder[String]                       = convert(_ => CStr)
  implicit def option[A: CSchemaEncoder]: CSchemaEncoder[Option[A]]        = convert(_ => COption(CSchemaEncoder[A].encode(null.asInstanceOf[A])))

  implicit val hnilEncoder: CStructEncoder[HNil] = convertS(_ => CStruct(Nil))

  implicit def hlistEncoder[K <: Symbol, H, T <: HList](
    implicit
    witness: Witness.Aux[K],
    hEncoder: Lazy[CSchemaEncoder[H]],
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
      val existing = hEncoder.value.encode(head).underlying
      val newInfo  = "type" -> CStr
      CStruct(newInfo :: existing)

    case Inr(tail) =>
      tEncoder.encode(tail)
  }
}

// See https://stackoverflow.com/questions/1886953/is-there-a-way-to-control-which-implicit-conversion-will-be-the-default-used
trait LowPriorityImplicits {
  // Given an encoder for the generic representation R, and a conversion from a specific representation A (case class)
  // to a generic representation R (HList) we can obtain an encoder for the specific representation (A)
  implicit def genericEncoder[A, R](
    implicit
    gen: LabelledGeneric.Aux[A, R],
    enc: Lazy[CStructEncoder[R]]
  ): CStructEncoder[A] =
    convertS { a =>
      enc.value.encode(gen.to(a))
    }
}
