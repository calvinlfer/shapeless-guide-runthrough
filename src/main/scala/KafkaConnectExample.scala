import org.apache.kafka.connect.data._
import java.math.{ BigDecimal => JBigDecimal }

import connect._

object KafkaConnectExample extends App {
  case class Circle(radius: Int)
  case class WhatThe(lol: OptionWithDefault[Circle])
  println {
    connect.CRep.struct {
      CRepEncoder[WhatThe].encode(
        WhatThe(OptionWithDefault(Option(Circle(10)), Circle(0)))
      )
    }
  }
//  val output = new Struct(schema)
//    .put("lol",
//         new Struct(schema.field("lol").schema())
//           .put("radius", 10))
//  println("LOL")
//  new Struct(schema)
//    .put("lol", new Struct())
//  sealed trait Shape
//  case class Circle(radius: Double)                extends Shape
//  case class Square(length: Double, width: Double) extends Shape
//  println {
//    structInterpreter {
//      CRepEncoder[Shape].encode(Circle(10))
//    }
//  }
//
//  val circleRep = CRepEncoder[Shape].encode(Circle(10))
//  println {
//    CRepDecoder[Shape].decode {
//      cRepInterpreter(
//        s = schemaInterpreter(circleRep),
//        st = structInterpreter(circleRep)
//      )
//    }
//  }
//
//  case class Book(name: String, pages: Int, color: String, `type`: String)
//  case class Student(name: String, id: Int, book: Book, tag: BigDecimal)
//  println {
//    structInterpreter {
//      CRepEncoder[Student].encode(Student("calvin", 1, Book("Category Theory", 367, "Blue", "Soft-cover"), BigDecimal(102, 2)))
//    }
//  }
//
//  val cSchema = CRepEncoder[Student].encode(Student("calvin", 1, Book("Category Theory", 367, "Blue", "Soft-cover"), JBigDecimal.valueOf(102, 2)))
//  println {
//    CRepDecoder[Student].decode(cSchema)
//  }
//
//  println {
//    cRepInterpreter(
//      schemaInterpreter(cSchema): Schema,
//      structInterpreter {
//        CRepEncoder[Student].encode(Student("calvin", 1, Book("Category Theory", 367, "Blue", "Soft-cover"), JBigDecimal.valueOf(102, 2)))
//      }: Struct
//    )
//  }
//
//  case class ArrayOfBytes(b: Array[Byte])
//  val example       = ArrayOfBytes("data".getBytes)
//  val exampleCRep   = CRepEncoder[ArrayOfBytes].encode(example)
//  val exampleStruct = structInterpreter(exampleCRep)
//  val exampleSchema = schemaInterpreter(exampleCRep)
//  val backToCRep    = cRepInterpreter(exampleSchema, exampleStruct)
//  println {
//    CRepDecoder[ArrayOfBytes].decode(backToCRep).map(a => new String(a.b))
//  }
}
