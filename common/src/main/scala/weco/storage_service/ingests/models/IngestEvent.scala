package weco.storage_service.ingests.models

import java.time.Instant

case class IngestEvent(description: String, createdDate: Instant = Instant.now)
