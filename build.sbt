name := "AirlineDelay"

version := "0.1"

scalaVersion := "2.12.13"

val sparkVersion = "3.0.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided,test",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided,test",

  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_1.0.0" % "test"
)

assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion}_${version.value}.jar"
