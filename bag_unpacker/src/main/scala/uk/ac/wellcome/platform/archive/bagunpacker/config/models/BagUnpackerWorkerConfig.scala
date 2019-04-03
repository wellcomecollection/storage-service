package uk.ac.wellcome.platform.archive.bagunpacker.config.models

case class BagUnpackerWorkerConfig(
  dstNamespace: String,
  maybeDstPrefix: Option[String] = None
)
