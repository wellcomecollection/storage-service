package uk.ac.wellcome.platform.archive.bag_register.models

import java.time.Instant

import uk.ac.wellcome.platform.archive.common.models.bagit.{BagId, BagLocation}
import uk.ac.wellcome.platform.archive.common.operation.Timed

case class RegistrationSummary(
  startTime: Instant,
  endTime: Option[Instant] = None,
  location: BagLocation,
  bagId: Option[BagId] = None
) extends Timed {
  def complete: RegistrationSummary = {
    this.copy(
      endTime = Some(Instant.now())
    )
  }
}
