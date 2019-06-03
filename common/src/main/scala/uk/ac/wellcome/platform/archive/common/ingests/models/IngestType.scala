package uk.ac.wellcome.platform.archive.common.ingests.models

import io.circe.CursorOp.DownField
import io.circe._

sealed trait IngestType { val id: String }

object CreateIngestType extends IngestType {
  override val id: String = "create"
}

object UpdateIngestType extends IngestType {
  override val id: String = "update"
}

object IngestType {
  implicit val decoder: Decoder[IngestType] = (cursor: HCursor) =>
    for {
      id <- cursor.downField("id").as[String]
      ingestType <- id match {
        case CreateIngestType.id => Right(CreateIngestType)
        case UpdateIngestType.id => Right(UpdateIngestType)
        case invalidId => val fields = DownField("id") +: cursor.history
          Left(DecodingFailure(
            s"""got "$invalidId", valid values are: ${CreateIngestType.id}, ${UpdateIngestType.id}.""",
            fields))
      }
    } yield ingestType

  implicit val encoder: Encoder[IngestType] =
    (ingestType: IngestType) => Json.obj("id" -> Json.fromString(ingestType.id))
}
