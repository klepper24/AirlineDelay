name := "SparkTraining"

version := "0.1"

scalaVersion := "2.12.13"

val sparkVersion = "3.1.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion}_${version.value}.jar"
