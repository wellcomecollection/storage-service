package uk.ac.wellcome.platform.archive.bagreplicator.replicator.azure

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.{Replicator, ReplicatorTestCases}
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.fixtures.{AzureFixtures, S3Fixtures}
import uk.ac.wellcome.storage.store.azure.{AzureStreamStore, AzureTypedStore}
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.azure.AzureBlobMetadata
import uk.ac.wellcome.storage.tags.s3.S3Tags
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

class AzureReplicatorTest
  extends ReplicatorTestCases[Bucket, Container]
    with AzureFixtures
    with S3Fixtures {

  override def withSrcNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  override def withDstNamespace[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def withReplicator[R](testWith: TestWith[Replicator, R]): R =
    testWith(new AzureReplicator())

  override def createSrcLocationWith(srcBucket: Bucket): ObjectLocation =
    createObjectLocationWith(srcBucket)

  override def createDstLocationWith(dstContainer: Container,
                                     path: String): ObjectLocation =
    createObjectLocationWith(dstContainer.name, path = path)

  override def createSrcPrefixWith(srcBucket: Bucket): ObjectLocationPrefix =
    ObjectLocationPrefix(srcBucket.name, path = "")

  override def createDstPrefixWith(
    dstContainer: Container): ObjectLocationPrefix =
    ObjectLocationPrefix(dstContainer.name, path = "")

  override def putSrcObject(location: ObjectLocation, contents: String): Unit =
    s3Client.putObject(location.namespace, location.path, contents)

  implicit val streamStore: AzureStreamStore = new AzureStreamStore()
  val typedStore = new AzureTypedStore[String]

  override def putDstObject(location: ObjectLocation, contents: String): Unit =
    typedStore.put(location)(contents)

  override def getDstObject(location: ObjectLocation): String =
    typedStore.get(location).right.value.identifiedT

  override  val srcTags: Tags[ObjectLocation] = new S3Tags()
  override  val dstTags: Tags[ObjectLocation] = new AzureBlobMetadata()
}
