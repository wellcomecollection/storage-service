package uk.ac.wellcome.platform.archive.common.ingests.models

import java.net.URI

import uk.ac.wellcome.platform.archive.common.IngestID

case class CallbackNotification(
  ingestId: IngestID,
  callbackUri: URI,
  payload: Ingest
)
