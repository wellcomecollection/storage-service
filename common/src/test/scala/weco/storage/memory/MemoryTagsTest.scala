package weco.storage.tags.memory

import java.util.UUID
import weco.fixtures.TestWith
import weco.storage.tags.{Tags, TagsTestCases}
import weco.storage.tags.TagsTestCases

class MemoryTagsTest extends TagsTestCases[UUID, Unit] {
  override def withTags[R](initialTags: Map[UUID, Map[String, String]])(
    testWith: TestWith[Tags[UUID], R]): R =
    testWith(
      new MemoryTags(initialTags)
    )

  override def createIdent(context: Unit): UUID = randomUUID

  override def withContext[R](testWith: TestWith[Unit, R]): R = testWith(())
}
