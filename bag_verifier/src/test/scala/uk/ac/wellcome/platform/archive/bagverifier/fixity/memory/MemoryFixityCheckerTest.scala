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
import uk.ac.wellcome.storage.tags.Tags
import uk.ac.wellcome.storage.tags.memory.MemoryTags

class MemoryFixityCheckerTest
    extends FixityCheckerTestCases[String, (MemoryStreamStore[ObjectLocation], MemoryTags[ObjectLocation])]
    with EitherValues {
  type MemoryContext = (MemoryStreamStore[ObjectLocation], MemoryTags[ObjectLocation])

  override def withContext[R](testWith: TestWith[MemoryContext, R]): R =
    testWith((
      MemoryStreamStore[ObjectLocation](),
      new MemoryTags[ObjectLocation](initialTags = Map.empty)
    ))

  override def putString(location: ObjectLocation, contents: String)(
    implicit context: MemoryContext
  ): Unit = {
    val (streamStore, _) = context
    val inputStream = stringCodec.toStream(contents).right.value
    streamStore.put(location)(inputStream) shouldBe a[Right[_, _]]
  }

  override def withFixityChecker[R](
    testWith: TestWith[FixityChecker, R]
  )(implicit context: MemoryContext): R = {
    val (streamStore, tags) = context
    testWith(
      new MemoryFixityChecker(streamStore, tags)
    )
  }

  override def withTags[R](testWith: TestWith[Tags[ObjectLocation], R])(
    implicit context: MemoryContext
  ): R = {
    val (_, tags) = context
    testWith(tags)
  }

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
