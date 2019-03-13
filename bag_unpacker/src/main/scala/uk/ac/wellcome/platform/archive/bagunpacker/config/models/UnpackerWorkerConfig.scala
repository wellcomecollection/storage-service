package uk.ac.wellcome.platform.archive.bagunpacker.config.models

case class UnpackerWorkerConfig(
  dstNamespace: String,
  maybeDstPrefix: Option[String] = None
)
