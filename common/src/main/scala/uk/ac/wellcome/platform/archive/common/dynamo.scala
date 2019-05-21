package uk.ac.wellcome.platform.archive.common

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.gu.scanamo.DynamoFormat
import com.gu.scanamo.error.DynamoReadError
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.vhs.{EmptyMetadata, Entry}

object dynamo {
  case class HybridRecord(
    id: String,
    version: Int,
    location: ObjectLocation
  )

  implicit val dynamoFormat: DynamoFormat[Entry[String, EmptyMetadata]] =
    new DynamoFormat[Entry[String, EmptyMetadata]] {
      override def read(av: AttributeValue)
        : Either[DynamoReadError, Entry[String, EmptyMetadata]] =
        DynamoFormat[HybridRecord].read(av).map { hybridRecord =>
          Entry(
            id = hybridRecord.id,
            version = hybridRecord.version,
            location = hybridRecord.location,
            metadata = EmptyMetadata()
          )
        }

      override def write(entry: Entry[String, EmptyMetadata]): AttributeValue =
        DynamoFormat[HybridRecord].write(
          HybridRecord(
            id = entry.id,
            version = entry.version,
            location = entry.location
          )
        )
    }

  implicit val updateExpressionGenerator
    : UpdateExpressionGenerator[Entry[String, EmptyMetadata]] =
    (entry: Entry[String, EmptyMetadata]) =>
      UpdateExpressionGenerator[HybridRecord].generateUpdateExpression(
        HybridRecord(
          id = entry.id,
          version = entry.version,
          location = entry.location
        )
    )
}
