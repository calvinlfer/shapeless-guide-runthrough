package connect
import java.math.{ BigDecimal => JBigDecimal }

import org.apache.kafka.connect.data._

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

object CRep {
  import scala.jdk.CollectionConverters._
  import org.apache.kafka.connect.data.Schema._

  def schemaInterpreter(c: CRep): Schema =
    c match {
      case _: CBool       => BOOLEAN_SCHEMA
      case _: CInt8       => INT8_SCHEMA
      case _: CBytes      => BYTES_SCHEMA
      case _: CInt16      => INT16_SCHEMA
      case CInt32(_)      => INT32_SCHEMA
      case CInt64(_)      => INT64_SCHEMA
      case CFloat32(_)    => FLOAT32_SCHEMA
      case CFloat64(_)    => FLOAT64_SCHEMA
      case CStr(_)        => STRING_SCHEMA
      case CCh(_)         => STRING_SCHEMA
      case CBigDecimal(b) => Decimal.builder(b.scale).build() // dynamically build out the schema using the BigDecimal
      case CStruct(underlying) =>
        underlying
          .foldLeft(SchemaBuilder.struct()) {
            case (acc, (fieldName, fieldSchema)) =>
              acc.field(fieldName, schemaInterpreter(fieldSchema))
          }
          .build()
    }

  def structInterpreter(c: CRep): Struct = {
    val schema = schemaInterpreter(c)

    def go(c: CRep): Struct =
      c match {
        case CStruct(underlying) =>
          underlying.foldLeft(new Struct(schema)) {
            case (acc, (fieldName, fieldSchema)) =>
              val unsafe: Any = fieldSchema match {
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
                case c @ CStruct(_) => structInterpreter(c)
              }
              acc.put(fieldName, unsafe)
          }
        case _ => new Struct(schema)
      }
    go(c)
  }

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
