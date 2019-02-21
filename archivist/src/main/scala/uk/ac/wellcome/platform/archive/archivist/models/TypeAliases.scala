package uk.ac.wellcome.platform.archive.archivist.models

import uk.ac.wellcome.platform.archive.common.models.{
  BagRequest,
  IngestBagRequest
}
import uk.ac.wellcome.platform.archive.common.models.error.ArchiveError

object TypeAliases {
  type BagDownload = Either[ArchiveError[_], FileDownloadComplete]
  type ArchiveCompletion = Either[ArchiveError[_], BagRequest]

  type IngestError = ArchiveError[IngestBagRequest]
}
