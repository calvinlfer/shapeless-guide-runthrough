import connect._
import CSchema._

object KafkaConnectExample extends App {
  sealed trait Shape
  case class Circle(radius: Double)                extends Shape
  case class Square(length: Double, width: Double) extends Shape

  case class Book(name: String, pages: Int, genre: String)
  case class Person(name: String, book: List[Book], tag: String, data: Option[Array[Byte]])

  println {
    schema {
      CSchemaEncoder[Shape].encode(Square(10, 20))
    }.fields
  }

  println {
    CSchemaEncoder[Option[Person]].encode(None)
  }
}
