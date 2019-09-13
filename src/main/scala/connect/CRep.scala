package connect
import java.math.{ BigDecimal => JBigDecimal }

import org.apache.kafka.connect.data._

case class OptionWithDefault[A](o: Option[A], default: A)

/**
 * CRep is our own intermediate well-typed combined representation of Kafka Connect's Schema and Struct which carries
 * around all the information we need to derive both pieces of Kafka Connect information.
 *
 * CRep is a Free structure that essentially provides a properly functioning Monoid on Tuple2[String, CRep] for the
 * case of CStruct which is not present in the target Kafka Connect Struct
 */
sealed trait CRep
final case class CInt8(b: Byte)                            extends CRep
final case class CBytes(a: Array[Byte])                    extends CRep
final case class CInt16(s: Short)                          extends CRep
final case class CInt32(i: Int)                            extends CRep
final case class CInt64(l: Long)                           extends CRep
final case class CFloat32(f: Float)                        extends CRep
final case class CFloat64(d: Double)                       extends CRep
final case class CCh(c: Char)                              extends CRep
final case class CStr(s: String)                           extends CRep
final case class CBool(b: Boolean)                         extends CRep
final case class CBigDecimal(b: JBigDecimal)               extends CRep
final case class CStruct(underlying: List[(String, CRep)]) extends CRep
final case class COpD(underlying: OptionWithDefault[CRep]) extends CRep

object CRep {
  import scala.jdk.CollectionConverters._

  def schemaInterpreter(c: CRep): Schema = {
    def go(c: CRep): SchemaBuilder =
      c match {
        case _: CBool       => SchemaBuilder.bool()
        case _: CInt8       => SchemaBuilder.int8()
        case _: CBytes      => SchemaBuilder.bytes()
        case _: CInt16      => SchemaBuilder.int16()
        case CInt32(_)      => SchemaBuilder.int32()
        case CInt64(_)      => SchemaBuilder.int64()
        case CFloat32(_)    => SchemaBuilder.float32()
        case CFloat64(_)    => SchemaBuilder.float64()
        case CStr(_)        => SchemaBuilder.string()
        case CCh(_)         => SchemaBuilder.string()
        case COpD(o)        => go(o.default).optional()
        case CBigDecimal(b) => Decimal.builder(b.scale) // dynamically build out the schema using the BigDecimal
        case CStruct(underlying) =>
          underlying
            .foldLeft(SchemaBuilder.struct()) {
              case (acc, (fieldName, fieldSchema)) =>
                acc.field(fieldName, go(fieldSchema).build())
            }
      }
    go(c).build()
  }

  def struct(c: CRep): Struct = {
    val schema = schemaInterpreter(c)
    def go(c: CRep, schema: Schema): Struct = {
      val struct = new Struct(schema)
      c match {
        case CStruct(underlying) =>
          underlying.foreach {
            case (fieldName, fieldSchema) =>
              fieldSchema match {
                // primitive types
                case CBool(x)       => struct.put(fieldName, x)
                case CInt8(x)       => struct.put(fieldName, x)
                case CBytes(x)      => struct.put(fieldName, x)
                case CInt16(x)      => struct.put(fieldName, x)
                case CInt32(x)      => struct.put(fieldName, x)
                case CInt64(x)      => struct.put(fieldName, x)
                case CFloat32(x)    => struct.put(fieldName, x)
                case CFloat64(x)    => struct.put(fieldName, x)
                case CStr(x)        => struct.put(fieldName, x)
                case CCh(x)         => struct.put(fieldName, x)
                case CBigDecimal(x) => struct.put(fieldName, x)

                // more complicated types
                case c: CStruct =>
                  go(c, schema.field(fieldName).schema())

                case COpD(OptionWithDefault(o, _)) =>
                  val wtf = o.map {
                    case CBool(x)       => x
                    case CInt8(x)       => x
                    case CBytes(x)      => x
                    case CInt16(x)      => x
                    case CInt32(x)      => x
                    case CInt64(x)      => x
                    case CFloat32(x)    => x
                    case CFloat64(x)    => x
                    case CStr(x)        => x
                    case CCh(x)         => x
                    case CBigDecimal(x) => x
                    case c: CStruct     => go(c, schema.field(fieldName).schema())
                    case c: COpD        => go(c, schema)
                  }.orNull
                  struct.put(fieldName, wtf)
              }
          }
      }
      struct
    }
    go(c, schema)
  }

//  def structInterpreter(c: CRep): Struct = {
//    val schema = schemaInterpreter(c)
//
//    def go(c: CRep, schema: Schema): Struct =
//      c match {
//        case CStruct(underlying) =>
//          underlying.foldLeft(new Struct(schema)) {
//            case (acc, (fieldName, fieldSchema)) =>
//              val unsafe: Any = fieldSchema match {
//                case CBool(x)       => x
//                case CInt8(x)       => x
//                case CBytes(x)      => x
//                case CInt16(x)      => x
//                case CInt32(x)      => x
//                case CInt64(x)      => x
//                case CFloat32(x)    => x
//                case CFloat64(x)    => x
//                case CStr(x)        => x
//                case CCh(x)         => x
//                case CBigDecimal(x) => x
//                case COpD(u)        => u.o.map(cRep => go(cRep, schema)).orNull
//                case c @ CStruct(_) => structInterpreter(c)
//              }
//              println(s"expecting to put in ${schema.fields}")
//              println(s"trying to put $fieldName in")
//              println(s"Status of trying to do this: ${schema.field(fieldName)}")
//              println(s"Putting in $unsafe")
//              acc.put(fieldName, unsafe)
//          }
//        case _ => new Struct(schema)
//      }
//    go(c, schema)
//  }

  import org.apache.kafka.connect.data.Schema.Type._
  def cRepInterpreter(s: Schema, st: Struct): CStruct =
    s.fields()
      .asScala
      .map { field =>
        val fieldName   = field.name()
        val schema      = field.schema()
        val connectType = schema.`type`()
        connectType match {
          case STRING =>
            CStruct((fieldName, CStr(st.getString(fieldName))) :: Nil)

          case STRUCT =>
            cRepInterpreter(schema, st.getStruct(fieldName))

          case INT8 =>
            CStruct((fieldName, CInt8(st.getInt8(fieldName))) :: Nil)

          case INT16 =>
            CStruct((fieldName, CInt16(st.getInt16(fieldName))) :: Nil)

          case INT32 =>
            CStruct((fieldName, CInt32(st.getInt32(fieldName))) :: Nil)

          case INT64 =>
            CStruct((fieldName, CInt64(st.getInt64(fieldName))) :: Nil)

          case FLOAT32 =>
            CStruct((fieldName, CFloat32(st.getFloat32(fieldName))) :: Nil)

          case FLOAT64 =>
            CStruct((fieldName, CFloat64(st.getFloat64(fieldName))) :: Nil)

          case BOOLEAN =>
            CStruct((fieldName, CBool(st.getBoolean(fieldName))) :: Nil)

          case BYTES =>
            val name = schema.name()
            name match {
              case Decimal.LOGICAL_NAME =>
                // doing it via getBytes and Decimal.toLogical doesn't seem to work :(
                val bd = st.get(field).asInstanceOf[java.math.BigDecimal]
                CStruct((fieldName, CBigDecimal(bd)) :: Nil)

              case _ =>
                val bytes = st.getBytes(fieldName)
                CStruct((fieldName, CBytes(bytes)) :: Nil)
            }
        }
      }
      .reduce((a, b) => CStruct(a.underlying ++ b.underlying))
}
