import java.math.{ BigDecimal => JBigDecimal }

import connect.CRep._
import connect._
import org.apache.kafka.connect.data._

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

  case class Haha(something: Int)
  case class Book(name: String, pages: Int, color: String, `type`: String, lol: Haha)
  case class Student(name: String, id: Int, book: Book, tag: BigDecimal)
  println {
    structInterpreter {
      CRepEncoder[Student].encode(Student("calvin", 1, Book("Category Theory", 367, "Blue", "Soft-cover", Haha(1)), BigDecimal(102, 2)))
    }
  }

  val cSchema = CRepEncoder[Student].encode(Student("calvin", 1, Book("Category Theory", 367, "Blue", "Soft-cover", Haha(2)), JBigDecimal.valueOf(102, 2)))
  println {
    CRepDecoder[Student].decode(cSchema)
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
  val encoded = CRepEncoder[Example].encode(Example(1, Some(Circle(10.0))))
  println(structInterpreter(encoded))
  println(
    structInterpreter {
      cRepInterpreter(schemaInterpreter(encoded), structInterpreter(encoded))
    }
  )

  val reEncoded = cRepInterpreter(schemaInterpreter(encoded), structInterpreter(encoded))
  println {
    CRepDecoder[Example].decode(reEncoded)
  }
}
