import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import Schema._
import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Witness}

object KafkaConnectSerializer extends App {
  sealed trait CSchema
  case class CInt8(b: Byte) extends CSchema
  case class CInt16(s: Short) extends CSchema
  case class CInt32(i: Int) extends CSchema
  case class CInt64(l: Long) extends CSchema
  case class CFloat32(f: Float) extends CSchema
  case class CFloat64(d: Double) extends CSchema
  case class CCh(c: Char) extends CSchema
  case class CStr(s: String) extends CSchema
  case class CStruct(underlying: List[(String, CSchema)]) extends CSchema

  trait ConnectSchemaEncoder[A] {
    def encode(a: A): CSchema
  }

  trait ConnectSchemaStructEncoder[A] extends ConnectSchemaEncoder[A] {
    def encode(a: A): CStruct
  }

  object ConnectSchemaEncoder {
    def apply[A](implicit C: ConnectSchemaEncoder[A]): ConnectSchemaEncoder[A] =
      C
    def convert[A](f: A => CSchema): ConnectSchemaEncoder[A] = (a: A) => f(a)
    def convertS[A](f: A => CStruct): ConnectSchemaStructEncoder[A] =
      (a: A) => f(a)

    implicit val byteEncoder: ConnectSchemaEncoder[Byte] = convert(CInt8)
    implicit val shortEncoder: ConnectSchemaEncoder[Short] = convert(CInt16)
    implicit val intEncoder: ConnectSchemaEncoder[Int] = convert(CInt32)
    implicit val longEncoder: ConnectSchemaEncoder[Long] = convert(CInt64)
    implicit val charEncoder: ConnectSchemaEncoder[Char] = convert(CCh)
    implicit val floatEncoder: ConnectSchemaEncoder[Float] = convert(CFloat32)
    implicit val doubleEncoder: ConnectSchemaEncoder[Double] = convert(CFloat64)
    implicit val stringEncoder: ConnectSchemaEncoder[String] = convert(CStr)

    implicit val hnilEncoder: ConnectSchemaStructEncoder[HNil] = convertS(
      _ => CStruct(Nil))

    implicit def hlistEncoder[K <: Symbol, H, T <: HList](
        implicit
        witness: Witness.Aux[K],
        hEncoder: Lazy[ConnectSchemaEncoder[H]],
        tEncoder: ConnectSchemaStructEncoder[T]
    ): ConnectSchemaStructEncoder[FieldType[K, H] :: T] = convertS {
      case h :: t =>
        val fieldName = witness.value.name
        val head = CStruct((fieldName, hEncoder.value.encode(h)) :: Nil)
        val tail = tEncoder.encode(t)
        CStruct(head.underlying ++ tail.underlying)
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

    def go(c: CSchema): Struct = {
      c match {
        case CStruct(underlying) =>
          underlying.foldLeft(new Struct(schema)) {
            case (acc, (fieldName, fieldSchema)) =>
              val unsafe: Any = fieldSchema match {
                case CInt8(x)    => x
                case CInt16(x)   => x
                case CInt32(x)   => x
                case CInt64(x)   => x
                case CFloat32(x) => x
                case CFloat64(x) => x
                case CStr(x)     => x
                case CCh(x)      => x
                case c @ CStruct(_)  => structInterpreter(c)
              }
              acc.put(fieldName, unsafe)
          }
        case _ => new Struct(schema)
      }
    }
    go(c)
  }

  case class Book(name: String, pages: Int)
  case class Student(name: String, id: Int, book: Book)
  println {
    structInterpreter {
      ConnectSchemaEncoder[Student].encode(Student("calvin", 1, Book("Category Theory", 367)))
    }
  }
}
