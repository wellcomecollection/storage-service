package weco.storage_service.ingests.models

import java.time.Instant

/** Records some processing activity on a bag, e.g. "started unpacking" or
  * "completed fixity checking".
  *
  */
case class IngestEvent(description: String, createdDate: Instant = Instant.now)
