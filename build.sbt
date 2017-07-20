organization in ThisBuild := "lagom-kinesis"
scalaVersion in ThisBuild := "2.11.8"

val slf4j = "org.slf4j" % "log4j-over-slf4j" % "1.7.21"
val akkaStreamKinesis = "com.gilt" %% "gfc-aws-kinesis-akka" % "0.12.1"
val awsJavaSdk = "com.amazonaws" % "aws-java-sdk" % "1.11.98"
val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"
val lagomApi = "com.lightbend.lagom" %% "lagom-api" % "1.3.0"
val lagomApiJavaDsl = "com.lightbend.lagom" %% "lagom-javadsl-api" % "1.3.0"
val lagomApiScalaDsl = "com.lightbend.lagom" %% "lagom-scaladsl-api" % "1.3.0"
val lagomPersistenceCore = "com.lightbend.lagom" %% "lagom-persistence-core" % "1.3.0"
val lagomJavadslBroker = "com.lightbend.lagom" %% "lagom-javadsl-broker" % "1.3.0"
val lagomJavadslServer = "com.lightbend.lagom" %% "lagom-javadsl-server" % "1.3.0"
val lagomScaladslBroker = "com.lightbend.lagom" %% "lagom-scaladsl-broker" % "1.3.0"
val lagomScaladslServer = "com.lightbend.lagom" %% "lagom-scaladsl-server" % "1.3.0"

val kinesisProjects = Seq[Project](
  `kinesis-client`,
  `kinesis-client-javadsl`,
  `kinesis-client-scaladsl`,
  `kinesis-broker`,
  `kinesis-broker-javadsl`,
  `kinesis-broker-scaladsl`
)

lazy val root = (project in file("."))
  .settings(name := "lagom-kinesis")
  .aggregate(kinesisProjects.map(Project.projectToRef): _*)

lazy val `kinesis-client` = (project in file("service/core/kinesis/client"))
  .settings(name := "lagom-kinesis-client")
  .settings(
    libraryDependencies ++= Seq(
      slf4j,
      akkaStreamKinesis,
      awsJavaSdk,
      lagomApi,
      scalaTest % Test
    )
  )

lazy val `kinesis-client-javadsl` = (project in file("service/javadsl/kinesis/client"))
  .settings(
    name := "lagom-javadsl-kinesis-client"
  )
  .settings(
    libraryDependencies ++= Seq(
      lagomApiJavaDsl
    )
  )
  .dependsOn(`kinesis-client`)

lazy val `kinesis-client-scaladsl` = (project in file("service/scaladsl/kinesis/client"))
  .settings(name := "lagom-scaladsl-kinesis-client")
  .settings(
    libraryDependencies ++= Seq(
      lagomApiScalaDsl
    )
  )
  .dependsOn(`kinesis-client`)

lazy val `kinesis-broker` = (project in file("service/core/kinesis/server"))
  .settings(name := "lagom-kinesis-broker")
  .settings(
    libraryDependencies ++= Seq(
      slf4j,
      akkaStreamKinesis,
      awsJavaSdk,
      lagomApi,
      lagomPersistenceCore,
      "org.mock-server" % "mockserver-netty" % "3.10.4"
    )
  )
  .dependsOn(`kinesis-client`)

lazy val `kinesis-broker-javadsl` = (project in file("service/javadsl/kinesis/server"))
  .settings(name := "lagom-javadsl-kinesis-broker")
  .settings(
    libraryDependencies ++= Seq(
      lagomApiJavaDsl,
      lagomJavadslBroker,
      lagomJavadslServer,
      scalaTest % Test
    )
  )
  .dependsOn(`kinesis-broker`, `kinesis-client-javadsl`)

lazy val `kinesis-broker-scaladsl` = (project in file("service/scaladsl/kinesis/server"))
  .settings(name := "lagom-scaladsl-kinesis-broker")
  .settings(
    libraryDependencies ++= Seq(
      lagomScaladslBroker,
      lagomScaladslServer,
      scalaTest % Test
    )
  )
  .dependsOn(`kinesis-broker`, `kinesis-client-scaladsl`)
