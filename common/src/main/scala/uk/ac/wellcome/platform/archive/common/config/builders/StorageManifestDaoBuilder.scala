package uk.ac.wellcome.platform.archive.common.config.builders

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.s3.AmazonS3
import com.typesafe.config.Config
import org.scanamo.auto._
//import org.scanamo.error.{DynamoReadError, TypeCoercionError}
//import org.scanamo.{DynamoFormat, DynamoValue}
import uk.ac.wellcome.json.JsonUtil._
//import uk.ac.wellcome.platform.archive.common.bagit.models.BagId
import uk.ac.wellcome.platform.archive.common.storage.models.StorageManifest
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestDao
//import uk.ac.wellcome.platform.archive.common.versioning.dynamo.DynamoID
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.s3.S3Config
import uk.ac.wellcome.storage.store.HybridIndexedStoreEntry
import uk.ac.wellcome.storage.store.dynamo.{DynamoHashRangeStore, DynamoHybridStoreWithMaxima, DynamoVersionedHybridStore}
import uk.ac.wellcome.storage.store.s3.{S3StreamStore, S3TypedStore}
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.typesafe.{DynamoBuilder, S3Builder}
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix, Version}

object StorageManifestDaoBuilder {
  def buildVHS(
    dynamoConfig: DynamoConfig,
    s3Config: S3Config
  )(
    implicit
    dynamoClient: AmazonDynamoDB,
    s3Client: AmazonS3
  ): DynamoVersionedHybridStore[String,
                                Int,
                                StorageManifest,
                                Map[String, String]] = {
    implicit val streamStore: S3StreamStore = new S3StreamStore()
    implicit val typedStore: S3TypedStore[StorageManifest] =
      new S3TypedStore[StorageManifest]()

    // TODO: Add a test for this
//    implicit val dynamoFormat: DynamoFormat[BagId] = new DynamoFormat[BagId] {
//      override def read(av: DynamoValue): Either[DynamoReadError, BagId] = {
//        val result = for {
//          stringId <- av.as[String]
//          space <- DynamoID.getStorageSpace(stringId).toEither
//          externalIdentifier <- DynamoID
//            .getExternalIdentifier(stringId)
//            .toEither
//          bagId = BagId(space, externalIdentifier)
//        } yield bagId
//
//        result match {
//          case Left(err) =>
//            Left(
//              TypeCoercionError(
//                new Throwable(s"Error decoding BagId from DynamoDB: $err")
//              ))
//          case Right(bagId) => Right(bagId)
//        }
//      }
//
//      override def write(bagId: BagId): DynamoValue =
//        DynamoValue.fromString(
//          DynamoID.createId(bagId.space, bagId.externalIdentifier)
//        )
//    }

    implicit val indexedStore
    : DynamoHashRangeStore[String, Int, HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Map[String, String]]] =
      new DynamoHashRangeStore[String, Int, HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Map[String, String]]](dynamoConfig)

    new DynamoVersionedHybridStore[
      String,
        Int,
        StorageManifest,
        Map[String, String]](
        store = new DynamoHybridStoreWithMaxima[
          String,
          Int,
          StorageManifest,
          Map[String, String]](
          prefix = ObjectLocationPrefix(
            namespace = s3Config.bucketName,
            path = ""
          )
        )
    )
  }

  def build(config: Config): StorageManifestDao = {

    // TODO: This should be a builder class
    implicit val dynamoClient: AmazonDynamoDB =
      DynamoBuilder.buildDynamoClient(config)

    implicit val s3Client: AmazonS3 =
      S3Builder.buildS3Client(config)

    new StorageManifestDao(buildVHS(
      dynamoConfig = DynamoBuilder.buildDynamoConfig(config, namespace = "vhs"),
      s3Config = S3Builder.buildS3Config(config, namespace = "vhs")
    ))
  }
}
