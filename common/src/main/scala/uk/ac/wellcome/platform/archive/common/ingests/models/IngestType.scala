package uk.ac.wellcome.platform.archive.common.ingests.models

import com.gu.scanamo.DynamoFormat
import io.circe.CursorOp.DownField
import io.circe._

import scala.util.{Failure, Success, Try}

sealed trait IngestType { val id: String }

object CreateIngestType extends IngestType {
  override val id: String = "create"
}

object UpdateIngestType extends IngestType {
  override val id: String = "update"
}

object IngestType {
  def create(id: String): IngestType =
    id match {
      case CreateIngestType.id => CreateIngestType
      case UpdateIngestType.id => UpdateIngestType
      case invalidId =>
        throw new Throwable(
          s"""got "$invalidId", valid values are: ${CreateIngestType.id}, ${UpdateIngestType.id}.""")
    }

  implicit val decoder: Decoder[IngestType] = (cursor: HCursor) =>
    for {
      id <- cursor.downField("id").as[String]
      ingestType <- Try { create(id) } match {
        case Success(ingestType) => Right(ingestType)
        case Failure(err) =>
          val fields = DownField("id") +: cursor.history
          Left(DecodingFailure(err.getMessage, fields))
      }
    } yield ingestType

  implicit val encoder: Encoder[IngestType] =
    (ingestType: IngestType) => Json.obj("id" -> Json.fromString(ingestType.id))

  implicit def format: DynamoFormat[IngestType] =
    DynamoFormat.coercedXmap[IngestType, String, IllegalArgumentException](
      id => IngestType.create(id)
    )(
      _.id
    )
}
