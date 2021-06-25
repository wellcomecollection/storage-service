package uk.ac.wellcome.platform.archive.bag_tracker.models

import java.time.Instant

import weco.storage_service.bagit.models.{BagId, BagVersion}

case class BagVersionEntry(
  version: BagVersion,
  createdDate: Instant
)

case class BagVersionList(
  id: BagId,
  versions: Seq[BagVersionEntry]
)
