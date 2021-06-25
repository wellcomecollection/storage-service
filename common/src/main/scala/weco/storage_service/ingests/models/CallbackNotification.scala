package weco.storage_service.ingests.models

import java.net.URI

case class CallbackNotification(
  ingestId: IngestID,
  callbackUri: URI,
  payload: Ingest
)
