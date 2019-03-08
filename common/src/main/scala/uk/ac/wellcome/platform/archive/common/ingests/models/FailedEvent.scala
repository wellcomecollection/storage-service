package uk.ac.wellcome.platform.archive.common.ingests.models

case class FailedEvent[T](e: Throwable, t: T)
