package uk.ac.wellcome.platform.archive.bagverifier.fixity.azure

import com.azure.storage.blob.BlobServiceClient
import org.scanamo.generic.auto._
import org.scanamo.syntax._
import org.scanamo.{Scanamo, Table}
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import uk.ac.wellcome.storage._
import weco.storage.azure.AzureBlobLocation
import weco.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.tags.Tags

import scala.language.higherKinds
import scala.util.{Failure, Success, Try}

// Although Azure Blob Storage supports S3-style tags, they're only in preview
// and not available in our region yet.
// https://docs.microsoft.com/en-us/azure/storage/blobs/storage-manage-find-blobs?tabs=azure-portal#regional-availability-and-storage-account-support
//
// Using DynamoDB to store tags about blobs in Azure is a stopgap until we get
// access to first-class Azure tags.
class AzureDynamoTags(dynamoConfig: DynamoConfig)(
  implicit
  blobServiceClient: BlobServiceClient,
  dynamoClient: DynamoDbClient
) extends Tags[AzureBlobLocation] {
  case class DynamoTagsEntry(id: String, tags: Map[String, String])

  private val table: Table[DynamoTagsEntry] =
    Table[DynamoTagsEntry](dynamoConfig.tableName)

  private val scanamo = Scanamo(dynamoClient)

  private def exists(location: AzureBlobLocation): Boolean =
    blobServiceClient
      .getBlobContainerClient(location.container)
      .getBlobClient(location.name)
      .exists()

  override protected def writeTags(
    location: AzureBlobLocation,
    tags: Map[String, String]
  ): Either[WriteError, Map[String, String]] =
    if (exists(location)) {
      val ops = if (tags.isEmpty) {
        table.delete("id" === location.toString())
      } else {
        table.put(DynamoTagsEntry(id = location.toString(), tags = tags))
      }

      Try {
        scanamo.exec(ops)
      } match {
        case Failure(err) =>
          Left(
            StoreWriteError(
              new Throwable(
                s"Error from Scanamo putting tags to $location: $err"
              )
            )
          )

        case Success(_) => Right(tags)
      }
    } else {
      Left(
        StoreWriteError(
          new Throwable(s"Location $location does not exist in Azure!")
        )
      )
    }

  override def get(location: AzureBlobLocation): ReadEither =
    if (exists(location)) {
      scanamo.exec(table.get("id" === location.toString)) match {
        case Some(Right(entry)) => Right(Identified(location, entry.tags))
        case None               => Right(Identified(location, Map.empty))
        case result =>
          Left(
            StoreReadError(
              new Throwable(
                s"Error from Scanamo looking up tags for $location: $result"
              )
            )
          )
      }
    } else {
      Left(DoesNotExistError())
    }
}
