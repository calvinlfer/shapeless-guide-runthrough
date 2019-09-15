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
final case class CInt8(b: Byte)                              extends CRep
final case class CBytes(a: Array[Byte])                      extends CRep
final case class CInt16(s: Short)                            extends CRep
final case class CInt32(i: Int)                              extends CRep
final case class CInt64(l: Long)                             extends CRep
final case class CFloat32(f: Float)                          extends CRep
final case class CFloat64(d: Double)                         extends CRep
final case class CCh(c: Char)                                extends CRep
final case class CStr(s: String)                             extends CRep
final case class CBool(b: Boolean)                           extends CRep
final case class CBigDecimal(b: JBigDecimal)                 extends CRep
final case class COption(value: Option[CRep], default: CRep) extends CRep
final case class CStruct(underlying: List[(String, CRep)])   extends CRep

object CRep {
  import scala.jdk.CollectionConverters._
  import org.apache.kafka.connect.data.Schema._

  private val OptionTypeKey    = "__type"
  private val OptionTypeValue  = "Option"
  private val OptionValueKey   = "__value"
  private val OptionDefaultKey = "__default"

  def schemaInterpreter(rep: CRep): Schema = {
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
        case CBigDecimal(b) => Decimal.builder(b.scale) // dynamically build out the schema using the BigDecimal
        case COption(_, default) =>
          val schemaOfUnderlyingType = go(default)
          SchemaBuilder
            .struct()
            .field(OptionTypeKey, STRING_SCHEMA)
            .field(OptionValueKey, schemaOfUnderlyingType.optional().build())
            .field(OptionDefaultKey, schemaOfUnderlyingType.build())

        case CStruct(underlying) =>
          underlying
            .foldLeft(SchemaBuilder.struct()) {
              case (acc, (fieldName, fieldSchema)) =>
                acc.field(fieldName, go(fieldSchema).build())
            }
      }

    go(rep).build()
  }

  def structInterpreter(c: CRep): Struct = {
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
                case COption(value, default) =>
                  // apply a rewrite to CStruct since we encode Option as CStruct
                  val schemaToPass = schema.field(fieldName).schema()
                  val convert = CStruct(
                    OptionTypeKey      -> CStr(OptionTypeValue) ::
                      OptionValueKey   -> value.orNull ::
                      OptionDefaultKey -> default ::
                      Nil
                  )
                  val place = go(convert, schemaToPass)
                  struct.put(fieldName, place)

                case c: CStruct =>
                  val place = go(c, schema.field(fieldName).schema())
                  struct.put(fieldName, place)

              }
          }
      }
      struct
    }
    go(c, schema)
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
            // handle option
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
