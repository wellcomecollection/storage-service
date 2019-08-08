val circeVersion = "0.8.0"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

addSbtPlugin("com.tapad" % "sbt-docker-compose" % "1.0.34")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.5")
addSbtPlugin("com.lucidchart" % "sbt-scalafmt" % "1.15")
addSbtPlugin("com.frugalmechanic" % "fm-sbt-s3-resolver" % "0.16.0")