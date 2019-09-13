package connect

import shapeless.{ ::, HList, HNil, LabelledGeneric, Lazy, Witness }
import shapeless.labelled.{ field, FieldType }
import java.math.{ BigDecimal => JBigDecimal }
import CRepDecoder.Error

trait CRepDecoder[A] {
  def decode(c: CRep): Either[Error, A]
}

object CRepDecoder {
  case class Error(message: String)

  // This is solely used for decoding CStructs
  private[connect] trait CStructDecoder[A] extends CRepDecoder[A] {
    def decode(c: CRep): Either[Error, A] = c match {
      case c: CStruct => decodeS(c)
      case e          => Left(Error(s"Expected CStruct but got $e"))
    }

    def decodeS(c: CStruct): Either[Error, A]
  }

  def apply[A](implicit C: CStructDecoder[A]): CStructDecoder[A] = C

  def convertBack[A](f: CRep => Either[Error, A]): CRepDecoder[A] = (c: CRep) => f(c)

  def convertBackS[A](f: CStruct => Either[Error, A]): CStructDecoder[A] = (c: CStruct) => f(c)

  implicit val intDecoder: CRepDecoder[Int] = convertBack {
    case CInt32(i) => Right(i)
    case e         => Left(Error(s"cannot convert $e to int"))
  }

  implicit val bigDecimalDecoder: CRepDecoder[BigDecimal] = convertBack {
    case CBigDecimal(b) => Right(b)
    case e              => Left(Error(s"cannot convert $e to BigDecimal"))
  }

  implicit val jBigDecimalDecoder: CRepDecoder[JBigDecimal] = convertBack {
    case CBigDecimal(b) => Right(b)
    case e              => Left(Error(s"cannot convert $e to BigDecimal"))
  }

  implicit val stringEncoder: CRepDecoder[String] = convertBack {
    case CStr(s) => Right(s)
    case e       => Left(Error(s"cannot convert $e to string"))
  }

  implicit val hnilEncoder: CStructDecoder[HNil] = convertBackS(_ => Right(HNil))

  implicit def hlistEncoder[K <: Symbol, H, T <: HList](
    implicit
    witness: Witness.Aux[K],
    hDecoder: Lazy[CRepDecoder[H]],
    tDecoder: CStructDecoder[T]
  ): CStructDecoder[FieldType[K, H] :: T] = convertBackS { cStruct =>
    val fieldName = witness.value.name
    val value: Either[Error, H] = {
      val raw = cStruct.underlying
        .find(_._1 == fieldName)
        .getOrElse(throw new Exception(s"Could not find field $fieldName"))
      hDecoder.value.decode(raw._2)
    }
    val tail: Either[Error, T] = tDecoder.decode(cStruct)
    for {
      h <- value
      t <- tail
    } yield field[K](h) :: t
  }

  implicit def genericDecoder[A, R](
    implicit
    gen: LabelledGeneric.Aux[A, R],
    enc: Lazy[CStructDecoder[R]]
  ): CStructDecoder[A] =
    convertBackS { cStruct =>
      val rEither = enc.value.decode(cStruct)
      rEither.map(r => gen.from(r))
    }
}
