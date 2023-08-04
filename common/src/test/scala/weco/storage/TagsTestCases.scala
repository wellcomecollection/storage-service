package weco.storage.tags

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.{RandomGenerators, TestWith}
import weco.storage.{
  DoesNotExistError,
  Identified,
  UpdateNoSourceError,
  UpdateNotApplied
}

trait TagsTestCases[Ident, Context]
    extends AnyFunSpec
    with Matchers
    with EitherValues
    with RandomGenerators {
  def withTags[R](initialTags: Map[Ident, Map[String, String]])(
    testWith: TestWith[Tags[Ident], R]): R

  def createIdent(context: Context): Ident

  val maxTags: Int = 25

  // One less than maxTags so we can append to the tags further down
  def createTags: Map[String, String] =
    collectionOf(min = 0, max = maxTags - 1) {
      randomAlphanumeric() -> randomAlphanumeric()
    }.toMap

  def withContext[R](testWith: TestWith[Context, R]): R

  describe("behaves as a Tags") {
    describe("get()") {
      it("can read the tags for an identifier") {
        withContext { context =>
          val objectIdent = createIdent(context)
          val objectTags = createTags

          withTags(initialTags = Map(objectIdent -> objectTags)) { tags =>
            tags.get(id = objectIdent).value shouldBe Identified(
              objectIdent,
              objectTags)
          }
        }
      }

      it("returns a DoesNotExistError if the ident does not exist") {
        withContext { context =>
          withTags(initialTags = Map.empty) { tags =>
            tags
              .get(id = createIdent(context))
              .left
              .value shouldBe a[DoesNotExistError]
          }
        }
      }
    }

    describe("update()") {
      it("appends to the tags on an object") {
        withContext { context =>
          val objectIdent = createIdent(context)
          val objectTags = createTags

          val newTag = Map("myTag" -> "newTag")

          withTags(initialTags = Map(objectIdent -> objectTags)) { tags =>
            tags
              .update(id = objectIdent) { existingTags =>
                Right(existingTags ++ newTag)
              }
              .value shouldBe Identified(objectIdent, objectTags ++ newTag)

            tags.get(id = objectIdent).value shouldBe Identified(
              objectIdent,
              objectTags ++ newTag)
          }
        }
      }

      it("can delete tags on an object") {
        withContext { context =>
          val objectIdent = createIdent(context)
          val objectTags = createTags

          withTags(initialTags = Map(objectIdent -> objectTags)) { tags =>
            tags
              .update(id = objectIdent) { existingTags =>
                Right(Map.empty)
              }
              .value shouldBe Identified(objectIdent, Map.empty)

            tags.get(id = objectIdent).value shouldBe Identified(
              objectIdent,
              Map.empty)
          }
        }
      }

      it("doesn't change the tags if the update function returns a Left()") {
        withContext { context =>
          val objectIdent = createIdent(context)
          val objectTags = createTags

          withTags(initialTags = Map(objectIdent -> objectTags)) { tags =>
            tags
              .update(id = objectIdent) { _ =>
                Left(UpdateNotApplied(new Throwable("BOOM!")))
              }
              .left
              .value shouldBe a[UpdateNotApplied]

            tags.get(id = objectIdent).value shouldBe Identified(
              objectIdent,
              objectTags)
          }
        }
      }

      it("can apply a no-op update on tags") {
        withContext { context =>
          val objectIdent = createIdent(context)
          val objectTags = createTags

          withTags(initialTags = Map(objectIdent -> objectTags)) { tags =>
            tags
              .update(id = objectIdent) {
                Right(_)
              }
              .value shouldBe Identified(objectIdent, objectTags)

            tags.get(id = objectIdent).value shouldBe Identified(
              objectIdent,
              objectTags)
          }
        }
      }

      it("returns an UpdateSourceError if the ident does not exist") {
        withContext { context =>
          val objectIdent = createIdent(context)

          withTags(initialTags = Map.empty) { tags =>
            tags
              .update(id = objectIdent) {
                Right(_)
              }
              .left
              .value shouldBe a[UpdateNoSourceError]

            tags.get(id = objectIdent).left.value shouldBe a[DoesNotExistError]
          }
        }
      }
    }
  }
}
