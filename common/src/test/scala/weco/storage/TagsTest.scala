package weco.storage.tags

import java.util.UUID

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.RandomGenerators
import weco.storage.tags.memory.MemoryTags
import weco.storage._

class TagsTest
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with RandomGenerators {
  def createTags: Map[String, String] =
    (1 to randomInt(from = 0, to = 25)).map { _ =>
      randomAlphanumeric() -> randomAlphanumeric()
    }.toMap

  def createIdent: UUID = randomUUID

  describe("update()") {
    it("wraps a ReadError from the underlying store") {
      val readError = StoreReadError(new Throwable("BOOM!"))

      class BrokenReadTags() extends MemoryTags[UUID](initialTags = Map.empty) {
        override def get(id: UUID): ReadEither =
          Left(readError)
      }

      val tags = new BrokenReadTags()

      tags
        .update(createIdent) { existingTags =>
          Right(existingTags ++ Map("myTag" -> "newValue"))
        }
        .left
        .value shouldBe UpdateReadError(readError)
    }

    it("wraps a WriteError from the underlying store") {
      val writeError = StoreWriteError(new Throwable("BOOM!"))

      class BrokenWriteTags(initialTags: Map[UUID, Map[String, String]])
          extends MemoryTags[UUID](initialTags = initialTags) {
        override protected def writeTags(
          id: UUID,
          tags: Map[String, String]): Either[WriteError, Map[String, String]] =
          Left(writeError)
      }

      val objectIdent = createIdent
      val objectTags = createTags

      val tags =
        new BrokenWriteTags(initialTags = Map(objectIdent -> objectTags))

      tags
        .update(objectIdent) { existingTags =>
          Right(existingTags ++ Map("myTag" -> "newValue"))
        }
        .left
        .value shouldBe UpdateWriteError(writeError)
    }
  }
}
