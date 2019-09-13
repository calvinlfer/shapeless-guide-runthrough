import org.apache.kafka.connect.data._
import java.math.{ BigDecimal => JBigDecimal }

import connect._
import connect.CRep._

object KafkaConnectExample extends App {
  sealed trait Shape
  case class Circle(radius: Double)                extends Shape
  case class Square(length: Double, width: Double) extends Shape
  println {
    structInterpreter {
      CRepEncoder[Shape].encode(Circle(10))
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
    cSchemaInterpreter(
      schemaInterpreter(cSchema): Schema,
      structInterpreter {
        CRepEncoder[Student].encode(Student("calvin", 1, Book("Category Theory", 367, "Blue", "Soft-cover"), JBigDecimal.valueOf(102, 2)))
      }: Struct
    )
  }
}
