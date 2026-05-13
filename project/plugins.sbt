
// Note this are provided by the base image in CI so they don't need to be pulled each run
// If you change these, make sure to update the base image as well (see the platform-infrastructure repository)
addSbtPlugin("com.tapad" % "sbt-docker-compose" % "1.0.35")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.10.4")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
addDependencyTreePlugin
