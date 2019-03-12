package uk.ac.wellcome.platform.archive.bag_register.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.bagit.models.{BagId, BagLocation}
import uk.ac.wellcome.platform.archive.common.operation.models.Summary

case class RegistrationSummary(
  startTime: Instant,
  endTime: Option[Instant] = None,
  location: BagLocation,
  bagId: Option[BagId] = None
) extends Summary {
  def complete: RegistrationSummary = {
    this.copy(
      endTime = Some(Instant.now())
    )
  }
  override def toString(): String = {
    f"""|${bagId.getOrElse("<unknown-bag>")}
        |registered in $formatDuration
       """.stripMargin
      .replaceAll("\n", " ")
  }
}
