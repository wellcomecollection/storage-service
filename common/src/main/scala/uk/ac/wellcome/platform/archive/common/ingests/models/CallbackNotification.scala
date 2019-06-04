package uk.ac.wellcome.platform.archive.common.ingests.models

import java.net.URI

case class CallbackNotification(
  ingestId: IngestID,
  callbackUri: URI,
  payload: Ingest
)
