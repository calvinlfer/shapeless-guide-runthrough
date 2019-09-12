import org.apache.kafka.connect.data.{ Schema, SchemaBuilder, Struct }
import Schema._
import shapeless.labelled.{ field, FieldType }
import shapeless.{ :+:, ::, CNil, Coproduct, HList, HNil, Inl, Inr, LabelledGeneric, Lazy, Witness }

object KafkaConnectSerializer extends App {
  sealed trait CSchema
  case class CInt8(b: Byte)                               extends CSchema
  case class CInt16(s: Short)                             extends CSchema
  case class CInt32(i: Int)                               extends CSchema
  case class CInt64(l: Long)                              extends CSchema
  case class CFloat32(f: Float)                           extends CSchema
  case class CFloat64(d: Double)                          extends CSchema
  case class CCh(c: Char)                                 extends CSchema
  case class CStr(s: String)                              extends CSchema
  case class CStruct(underlying: List[(String, CSchema)]) extends CSchema

  trait ConnectSchemaEncoder[A] {
    def encode(a: A): CSchema
  }

  trait ConnectSchemaStructEncoder[A] extends ConnectSchemaEncoder[A] {
    def encode(a: A): CStruct
  }

  case class Error(message: String)

  trait ConnectSchemaDecoder[A] {
    def decode(c: CSchema): Either[Error, A]
  }

  trait ConnectSchemaStructDecoder[A] extends ConnectSchemaDecoder[A] {
    def decode(c: CSchema): Either[Error, A] = c match {
      case c: CStruct => decodeS(c)
      case e          => Left(Error(s"Expected CStruct but got $e"))
    }

    def decodeS(c: CStruct): Either[Error, A]
  }

  object ConnectSchemaDecoder {
    def apply[A](implicit C: ConnectSchemaStructDecoder[A]): ConnectSchemaStructDecoder[A] = C
    def convertBack[A](f: CSchema => Either[Error, A]): ConnectSchemaDecoder[A] =
      (c: CSchema) => f(c)
    def convertBackS[A](f: CStruct => Either[Error, A]): ConnectSchemaStructDecoder[A] =
      (c: CStruct) => f(c)

    implicit val intEncoder: ConnectSchemaDecoder[Int] = convertBack {
      case CInt32(i) => Right(i)
      case e         => Left(Error(s"cannot convert $e to int"))
    }

    implicit val stringEncoder: ConnectSchemaDecoder[String] = convertBack {
      case CStr(s) => Right(s)
      case e       => Left(Error(s"cannot convert $e to string"))
    }

    implicit val hnilEncoder: ConnectSchemaStructDecoder[HNil] = convertBackS(_ => Right(HNil))

    implicit def hlistEncoder[K <: Symbol, H, T <: HList](
      implicit
      witness: Witness.Aux[K],
      hDecoder: Lazy[ConnectSchemaDecoder[H]],
      tDecoder: ConnectSchemaStructDecoder[T]
    ): ConnectSchemaStructDecoder[FieldType[K, H] :: T] = convertBackS { cStruct =>
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
      enc: Lazy[ConnectSchemaStructDecoder[R]]
    ): ConnectSchemaStructDecoder[A] =
      convertBackS { cStruct =>
        val rEither = enc.value.decode(cStruct)
        rEither.map(r => gen.from(r))
      }
  }

  object ConnectSchemaEncoder {
    // summoner
    def apply[A](implicit C: ConnectSchemaEncoder[A]): ConnectSchemaEncoder[A] =
      C
    def convert[A](f: A => CSchema): ConnectSchemaEncoder[A] = (a: A) => f(a)
    def convertS[A](f: A => CStruct): ConnectSchemaStructEncoder[A] =
      (a: A) => f(a)

    implicit val byteEncoder: ConnectSchemaEncoder[Byte]     = convert(CInt8)
    implicit val shortEncoder: ConnectSchemaEncoder[Short]   = convert(CInt16)
    implicit val intEncoder: ConnectSchemaEncoder[Int]       = convert(CInt32)
    implicit val longEncoder: ConnectSchemaEncoder[Long]     = convert(CInt64)
    implicit val charEncoder: ConnectSchemaEncoder[Char]     = convert(CCh)
    implicit val floatEncoder: ConnectSchemaEncoder[Float]   = convert(CFloat32)
    implicit val doubleEncoder: ConnectSchemaEncoder[Double] = convert(CFloat64)
    implicit val stringEncoder: ConnectSchemaEncoder[String] = convert(CStr)

    implicit val hnilEncoder: ConnectSchemaStructEncoder[HNil] = convertS(_ => CStruct(Nil))

    implicit def hlistEncoder[K <: Symbol, H, T <: HList](
      implicit
      witness: Witness.Aux[K],
      hEncoder: Lazy[ConnectSchemaEncoder[H]],
      tEncoder: ConnectSchemaStructEncoder[T]
    ): ConnectSchemaStructEncoder[FieldType[K, H] :: T] = convertS {
      case h :: t =>
        val fieldName = witness.value.name
        val head      = (fieldName, hEncoder.value.encode(h))
        val tail      = tEncoder.encode(t)
        CStruct(head :: tail.underlying)
    }

    implicit val cnilEncoder: ConnectSchemaStructEncoder[CNil] =
      convertS(_ => throw new Exception("This cannot happen as CNil is equivalent to Nothing and has no type inhabitants"))

    // NOTE: I'm expecting a sealed trait whose subtypes are case classes so they will only produce CStructs
    // So I use hEncoder: Lazy[ConnectSchemaStructEncoder[H]], instead of ConnectSchemaEncoder like we did in HList
    // here a Witness on K refers to the name of the sealed trait subtype
    implicit def coproductEncoder[K <: Symbol, H, T <: Coproduct](
      implicit
      witness: Witness.Aux[K],
      hEncoder: Lazy[ConnectSchemaStructEncoder[H]],
      tEncoder: ConnectSchemaStructEncoder[T]
    ): ConnectSchemaStructEncoder[FieldType[K, H] :+: T] = convertS {
      case Inl(head) =>
        val typeName = witness.value.name
        val existing = hEncoder.value.encode(head).underlying
        val newInfo  = "type" -> CStr(typeName)
        CStruct(newInfo :: existing)

      case Inr(tail) =>
        tEncoder.encode(tail)
    }

    implicit def genericEncoder[A, R](
      implicit
      gen: LabelledGeneric.Aux[A, R],
      enc: Lazy[ConnectSchemaStructEncoder[R]]
    ): ConnectSchemaStructEncoder[A] =
      convertS(a => enc.value.encode(gen.to(a)))
  }

  def schemaInterpreter(c: CSchema): Schema =
    c match {
      case _: CInt8    => INT8_SCHEMA
      case _: CInt16   => INT16_SCHEMA
      case CInt32(_)   => INT32_SCHEMA
      case CInt64(_)   => INT64_SCHEMA
      case CFloat32(_) => FLOAT32_SCHEMA
      case CFloat64(_) => FLOAT64_SCHEMA
      case CStr(_)     => STRING_SCHEMA
      case CCh(_)      => STRING_SCHEMA
      case CStruct(underlying) =>
        underlying
          .foldLeft(SchemaBuilder.struct()) {
            case (acc, (fieldName, fieldSchema)) =>
              acc.field(fieldName, schemaInterpreter(fieldSchema))
          }
          .build()
    }

  def structInterpreter(c: CSchema): Struct = {
    val schema = schemaInterpreter(c)

    def go(c: CSchema): Struct =
      c match {
        case CStruct(underlying) =>
          underlying.foldLeft(new Struct(schema)) {
            case (acc, (fieldName, fieldSchema)) =>
              val unsafe: Any = fieldSchema match {
                case CInt8(x)       => x
                case CInt16(x)      => x
                case CInt32(x)      => x
                case CInt64(x)      => x
                case CFloat32(x)    => x
                case CFloat64(x)    => x
                case CStr(x)        => x
                case CCh(x)         => x
                case c @ CStruct(_) => structInterpreter(c)
              }
              acc.put(fieldName, unsafe)
          }
        case _ => new Struct(schema)
      }
    go(c)
  }

  sealed trait Shape
  case class Circle(radius: Double)                extends Shape
  case class Square(length: Double, width: Double) extends Shape
  println {
    structInterpreter {
      ConnectSchemaEncoder[Shape].encode(Circle(10))
    }
  }

  case class Book(name: String, pages: Int, color: String, `type`: String)
  case class Student(name: String, id: Int, book: Book)
  println {
    structInterpreter {
      ConnectSchemaEncoder[Student].encode(Student("calvin", 1, Book("Category Theory", 367, "Blue", "Soft-cover")))
    }
  }

  val cSchema = ConnectSchemaEncoder[Student].encode(Student("calvin", 1, Book("Category Theory", 367, "Blue", "Soft-cover")))
  println {
    ConnectSchemaDecoder[Student].decode(cSchema)
  }
}
