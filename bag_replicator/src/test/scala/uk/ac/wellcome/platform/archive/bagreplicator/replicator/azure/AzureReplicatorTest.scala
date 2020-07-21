package uk.ac.wellcome.platform.archive.bagreplicator.replicator.azure

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagreplicator.replicator.{
  Replicator,
  ReplicatorTestCases
}
import uk.ac.wellcome.storage.fixtures.AzureFixtures
import uk.ac.wellcome.storage.fixtures.AzureFixtures.Container
import uk.ac.wellcome.storage.store.azure.{AzureStreamStore, AzureTypedStore}
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.azure.AzureBlobMetadata
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

class AzureReplicatorTest
    extends ReplicatorTestCases[Container]
    with AzureFixtures {

  override def withDstNamespace[R](testWith: TestWith[Container, R]): R =
    withAzureContainer { container =>
      testWith(container)
    }

  override def withReplicator[R](testWith: TestWith[Replicator, R]): R =
    testWith(new AzureReplicator())

  override def createDstLocationWith(
    dstContainer: Container,
    path: String
  ): ObjectLocation =
    createObjectLocationWith(dstContainer.name, path = path)

  override def createDstPrefixWith(
    dstContainer: Container
  ): ObjectLocationPrefix =
    ObjectLocationPrefix(dstContainer.name, path = "")

  implicit val streamStore: AzureStreamStore = new AzureStreamStore()
  override val dstStringStore: AzureTypedStore[String] =
    new AzureTypedStore[String]

  override val dstTags: Tags[ObjectLocation] = new AzureBlobMetadata()
}
