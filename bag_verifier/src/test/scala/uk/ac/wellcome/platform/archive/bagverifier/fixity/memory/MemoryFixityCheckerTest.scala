package uk.ac.wellcome.platform.archive.bagverifier.fixity.memory

import java.net.URI

import org.scalatest.EitherValues
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.platform.archive.bagverifier.fixity.{
  FixityChecker,
  FixityCheckerTestCases
}
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.memory.MemoryStreamStore
import uk.ac.wellcome.storage.streaming.Codec._

class MemoryFixityCheckerTest
    extends FixityCheckerTestCases[String, MemoryStreamStore[ObjectLocation]]
    with EitherValues {
  override def withContext[R](
    testWith: TestWith[MemoryStreamStore[ObjectLocation], R]
  ): R =
    testWith(
      MemoryStreamStore[ObjectLocation]()
    )

  override def putString(location: ObjectLocation, contents: String)(
    implicit streamStore: MemoryStreamStore[ObjectLocation]
  ): Unit = {
    val inputStream = stringCodec.toStream(contents).right.value
    streamStore.put(location)(inputStream) shouldBe a[Right[_, _]]
  }

  override def withFixityChecker[R](
    testWith: TestWith[FixityChecker, R]
  )(implicit streamStore: MemoryStreamStore[ObjectLocation]): R =
    testWith(
      new MemoryFixityChecker(streamStore)
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
