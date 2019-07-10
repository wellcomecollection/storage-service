package uk.ac.wellcome.platform.archive.common.verify.memory

import java.net.URI

import org.scalatest.EitherValues
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.common.verify.{
  Verifier,
  VerifierTestCases
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

class MemoryVerifierTest
    extends VerifierTestCases[String, MemoryStreamStore[ObjectLocation]]
    with EitherValues {
  override def withContext[R](
    testWith: TestWith[MemoryStreamStore[ObjectLocation], R]): R =
    testWith(
      MemoryStreamStore[ObjectLocation]()
    )

  override def putString(location: ObjectLocation, contents: String)(
    implicit streamStore: MemoryStreamStore[ObjectLocation]): Unit =
    streamStore.put(location)(
      InputStreamWithLengthAndMetadata(
        stringCodec.toStream(contents).right.value,
        metadata = Map.empty
      )
    ) shouldBe a[Right[_, _]]

  override def withVerifier[R](testWith: TestWith[Verifier[_], R])(
    implicit streamStore: MemoryStreamStore[ObjectLocation]): R =
    testWith(
      new MemoryVerifier(streamStore)
    )

  override def createObjectLocationWith(namespace: String): ObjectLocation =
    ObjectLocation(
      namespace = namespace,
      path = randomAlphanumeric
    )

  override def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric)

  override def createId(implicit namespace: String): ObjectLocation =
    createObjectLocationWith(namespace)

  override def resolve(location: ObjectLocation): URI =
    new URI(s"mem://${location.namespace}/${location.path}")
}
