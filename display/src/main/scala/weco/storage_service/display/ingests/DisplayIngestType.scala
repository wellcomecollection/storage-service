package weco.storage_service.display.ingests

import io.circe.{Decoder, Encoder, HCursor, Json}
import weco.storage_service.ingests.models.{
  CreateIngestType,
  IngestType,
  UpdateIngestType
}

sealed trait DisplayIngestType { val id: String }

object CreateDisplayIngestType extends DisplayIngestType {
  override val id: String = "create"
}

object UpdateDisplayIngestType extends DisplayIngestType {
  override val id: String = "update"
}

object DisplayIngestType {
  def apply(ingestType: IngestType): DisplayIngestType =
    ingestType match {
      case CreateIngestType => CreateDisplayIngestType
      case UpdateIngestType => UpdateDisplayIngestType
    }

  implicit val decoder: Decoder[DisplayIngestType] =
    (cursor: HCursor) =>
      Decoder[IngestType]
        .apply(cursor)
        .map {
          DisplayIngestType(_)
      }

  implicit val encoder: Encoder[DisplayIngestType] =
    (ingestType: DisplayIngestType) =>
      Json.obj(
        "id" -> Json.fromString(ingestType.id),
        "type" -> Json.fromString("IngestType")
    )
}
