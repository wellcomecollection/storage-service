package uk.ac.wellcome.platform.archive.common.ingests.models

import java.net.URI
import java.util.UUID

case class CallbackNotification(id: UUID, callbackUri: URI, payload: Ingest)
