package uk.ac.wellcome.platform.archive.common.ingests.models

import java.util.UUID
import uk.ac.wellcome.storage.TypedStringScanamoOps
import weco.json.TypedString

class IngestID(val uuid: UUID) extends TypedString[IngestID] {
  override val underlying: String = uuid.toString
}

object IngestID extends TypedStringScanamoOps[IngestID] {
  def random: IngestID = new IngestID(UUID.randomUUID())

  override def apply(id: String): IngestID =
    new IngestID(UUID.fromString(id))

  def apply(uuid: UUID): IngestID =
    new IngestID(uuid)
}
