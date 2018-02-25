name := """daf-service-storage-new"""
organization := "it.gov.daf"

version := "1.0-SNAPSHOT"

resolvers ++= Seq(
  "maven" at "https://repo1.maven.org/maven2/",
  "cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/",
  "daf" at "http://nexus.daf.teamdigitale.it/repository/maven-public/"
)

lazy val root = (project in file("."))
  .settings(
    scalaVersion := "2.11.12"
  )
  .enablePlugins(
    PlayScala
  )


libraryDependencies ++= Seq(
  guice,
  ws,
  ehcache,
  "io.swagger" % "swagger-play2_2.11" % "1.6.0",
  "org.webjars" %% "webjars-play" % "2.6.3",
  "org.webjars" % "swagger-ui" % "3.10.0",
  "com.twitter" %% "util-collection" % "18.2.0",

  "org.apache.spark" % "spark-core_2.11" % "2.2.0.cloudera1",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0.cloudera1",
  "com.databricks" %% "spark-avro" % "4.0.0",

  "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test,
  "de.leanovate.play-mockws" %% "play-mockws" % "2.6.2" % Test,
  "org.mockito" % "mockito-core" % "2.10.0" % Test
)

libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }


// Adds additional packages into Twirl
//TwirlKeys.templateImports += "it.gov.daf.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "it.gov.daf.binders._"
