import org.apache.kafka.connect.data._
import java.math.{ BigDecimal => JBigDecimal }

import connect._
import connect.CRep._
import shapeless.{ <:!<, Refute }

object KafkaConnectExample extends App {
  sealed trait Shape
  case class Circle(radius: Double)                extends Shape
  case class Square(length: Double, width: Double) extends Shape
  println {
    structInterpreter {
      CRepEncoder[Shape].encode(Circle(10))
    }
  }

  val circleRep = CRepEncoder[Shape].encode(Circle(10))
  println {
    CRepDecoder[Shape].decode {
      cRepInterpreter(
        s = schemaInterpreter(circleRep),
        st = structInterpreter(circleRep)
      )
    }
  }

  case class Book(name: String, pages: Int, color: String, `type`: String)
  case class Student(name: String, id: Int, book: Book, tag: BigDecimal)
  println {
    structInterpreter {
      CRepEncoder[Student].encode(Student("calvin", 1, Book("Category Theory", 367, "Blue", "Soft-cover"), BigDecimal(102, 2)))
    }
  }

  val cSchema = CRepEncoder[Student].encode(Student("calvin", 1, Book("Category Theory", 367, "Blue", "Soft-cover"), JBigDecimal.valueOf(102, 2)))
  println {
    CRepDecoder[Student].decode(cSchema)
  }

  println {
    cRepInterpreter(
      schemaInterpreter(cSchema): Schema,
      structInterpreter {
        CRepEncoder[Student].encode(Student("calvin", 1, Book("Category Theory", 367, "Blue", "Soft-cover"), JBigDecimal.valueOf(102, 2)))
      }: Struct
    )
  }

  case class ArrayOfBytes(b: Array[Byte])
  val example       = ArrayOfBytes("data".getBytes)
  val exampleCRep   = CRepEncoder[ArrayOfBytes].encode(example)
  val exampleStruct = structInterpreter(exampleCRep)
  val exampleSchema = schemaInterpreter(exampleCRep)
  val backToCRep    = cRepInterpreter(exampleSchema, exampleStruct)
  println {
    CRepDecoder[ArrayOfBytes].decode(backToCRep).map(a => new String(a.b))
  }

  implicit val os: CRepEncoder[Option[Circle]] = CRepEncoder.optionEncoder(Circle(0.0))
  case class Example(a: Int, b: Option[Circle])
  val encoded = CRepEncoder[Example].encode(Example(1, None))
  println {
    CRepDecoder[Example].decode(encoded)
  }
}
