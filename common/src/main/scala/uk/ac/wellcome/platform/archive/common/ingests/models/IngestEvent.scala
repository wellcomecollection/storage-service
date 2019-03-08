package uk.ac.wellcome.platform.archive.common.ingests.models

import java.time.Instant

case class IngestEvent(description: String,
                       createdDate: Instant = Instant.now)
