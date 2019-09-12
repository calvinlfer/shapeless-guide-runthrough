name := "shapeless-guide"

version := "0.1"

scalaVersion := "2.13.0"

libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.3.3",
  "org.apache.kafka" % "connect-api" % "2.3.0"
)
addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")
