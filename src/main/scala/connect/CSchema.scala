package connect

import org.apache.kafka.connect.data.{ Decimal, Schema, SchemaBuilder }

/**
 * CRep is our own intermediate well-typed combined representation of Kafka Connect's Schema and Struct which carries
 * around all the information we need to derive both pieces of Kafka Connect information.
 *
 * CRep is a Free structure that essentially provides a properly functioning Monoid on Tuple2[String, CRep] for the
 * case of CStruct which is not present in the target Kafka Connect Struct
 */
sealed trait CSchema
case object CInt8                                             extends CSchema
case object CBytes                                            extends CSchema
case object CInt16                                            extends CSchema
case object CInt32                                            extends CSchema
case object CInt64                                            extends CSchema
case object CFloat32                                          extends CSchema
case object CFloat64                                          extends CSchema
case object CCh                                               extends CSchema
case object CStr                                              extends CSchema
case object CBool                                             extends CSchema
final case class CBigDecimal(scale: Int)                      extends CSchema
final case class CStruct(underlying: List[(String, CSchema)]) extends CSchema
final case class COption(underlying: CSchema)                 extends CSchema

object CSchema {
  def schema(c: CSchema): Schema = {
    def go(c: CSchema): SchemaBuilder =
      c match {
        case CBool               => SchemaBuilder.bool()
        case CBytes              => SchemaBuilder.bytes()
        case CInt8               => SchemaBuilder.int8()
        case CInt16              => SchemaBuilder.int16()
        case CInt32              => SchemaBuilder.int32()
        case CInt64              => SchemaBuilder.int64()
        case CFloat32            => SchemaBuilder.float32()
        case CFloat64            => SchemaBuilder.float64()
        case CStr                => SchemaBuilder.string()
        case CCh                 => SchemaBuilder.string()
        case CBigDecimal(scale)  => Decimal.builder(scale)
        case COption(underlying) => go(underlying)
        case CStruct(underlying) =>
          underlying
            .foldLeft(SchemaBuilder.struct()) {
              case (acc, (fieldName, fieldSchema)) =>
                acc.field(fieldName, go(fieldSchema).build())
            }
      }
    go(c).build()
  }
}
