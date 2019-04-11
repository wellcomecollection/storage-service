package uk.ac.wellcome.platform.archive.bagverifier.services

import org.apache.commons.codec.digest.MessageDigestAlgorithms
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.akka.fixtures.Akka
import uk.ac.wellcome.platform.archive.bagverifier.models.VerificationSummary
import uk.ac.wellcome.platform.archive.common.fixtures.{
  BagLocationFixtures,
  FileEntry
}
import uk.ac.wellcome.platform.archive.common.storage.models.{
  IngestFailed,
  IngestStepSucceeded
}
import uk.ac.wellcome.platform.archive.common.storage.services.StorageManifestService
import uk.ac.wellcome.storage.fixtures.S3

class VerifierTest
    extends FunSpec
    with Matchers
    with ScalaFutures
    with S3
    with Akka
    with BagLocationFixtures {

  val dataFileCount = 3

  // Data files plus manifest-sha256.txt, bagit.txt, bag-info.txt
  val expectedDataFileCount = dataFileCount + 3

  it("passes a bag with correct checksums") {
    withLocalS3Bucket { bucket =>
      withBag(bucket, dataFileCount = dataFileCount) { bagLocation =>
        withActorSystem { actorSystem =>
          implicit val ec = actorSystem.dispatcher

          withMaterializer { mat =>
            implicit val _mat = mat

            val service = new Verifier(
              storageManifestService = new StorageManifestService(),
              s3Client = s3Client,
              algorithm = MessageDigestAlgorithms.SHA_256
            )

            val future = service.verify(bagLocation)

            whenReady(future) { result =>
              result shouldBe a[IngestStepSucceeded[_]]

              val summary = result.summary

              summary.successfulVerifications should have size expectedDataFileCount
              summary.failedVerifications shouldBe Seq.empty
            }
          }
        }
      }
    }
  }

  it("passes a bag in a sub directory with correct checksums") {
    withLocalS3Bucket { bucket =>
      withBag(bucket = bucket, dataFileCount = dataFileCount, bagRootDirectory = Some("bag")) { bagLocation =>
        withActorSystem { actorSystem =>
          implicit val ec = actorSystem.dispatcher

          withMaterializer { mat =>
            implicit val _mat = mat

            val service = new Verifier(
              storageManifestService = new StorageManifestService(),
              s3Client = s3Client,
              algorithm = MessageDigestAlgorithms.SHA_256
            )

            val future = service.verify(bagLocation)

            whenReady(future) { result =>
              result shouldBe a[IngestStepSucceeded[_]]

              val summary = result.summary

              summary.successfulVerifications should have size expectedDataFileCount
              summary.failedVerifications shouldBe Seq.empty
            }
          }
        }
      }
    }
  }

  it("fails a bag with an incorrect checksum in the file manifest") {
    withLocalS3Bucket { bucket =>
      withBag(
        bucket,
        dataFileCount = dataFileCount,
        createDataManifest = dataManifestWithWrongChecksum) { bagLocation =>
        withActorSystem { actorSystem =>
          implicit val ec = actorSystem.dispatcher
          withMaterializer { mat =>
            implicit val _mat = mat

            val service = new Verifier(
              storageManifestService = new StorageManifestService(),
              s3Client = s3Client,
              algorithm = MessageDigestAlgorithms.SHA_256
            )

            val future = service.verify(bagLocation)

            whenReady(future) { result =>
              result shouldBe a[IngestFailed[_]]

              val summary = result.summary
              summary.successfulVerifications should have size expectedDataFileCount - 1
              summary.failedVerifications should have size 1

              val brokenFile = summary.failedVerifications.head
              brokenFile.reason shouldBe a[RuntimeException]
              brokenFile.reason.getMessage should startWith(
                "Checksums do not match:")
            }
          }
        }
      }
    }
  }

  it("fails a bag with an incorrect checksum in the tag manifest") {
    withLocalS3Bucket { bucket =>
      withBag(
        bucket,
        dataFileCount = dataFileCount,
        createTagManifest = tagManifestWithWrongChecksum) { bagLocation =>
        withActorSystem { actorSystem =>
          implicit val ec = actorSystem.dispatcher
          withMaterializer { mat =>
            implicit val _mat = mat

            val service = new Verifier(
              storageManifestService = new StorageManifestService(),
              s3Client = s3Client,
              algorithm = MessageDigestAlgorithms.SHA_256
            )
            val future = service.verify(bagLocation)
            whenReady(future) { result =>
              result shouldBe a[IngestFailed[_]]

              val summary = result.summary

              summary.successfulVerifications should have size expectedDataFileCount - 1
              summary.failedVerifications should have size 1

              val brokenFile = summary.failedVerifications.head
              brokenFile.reason shouldBe a[RuntimeException]
              brokenFile.reason.getMessage should startWith(
                "Checksums do not match:")
            }
          }
        }
      }
    }
  }

  it("fails a bag if the file manifest refers to a non-existent file") {
    def createDataManifestWithExtraFile(
      dataFiles: List[(String, String)]): Option[FileEntry] =
      createValidDataManifest(
        dataFiles ++ List(("doesnotexist", "doesnotexist")))

    withLocalS3Bucket { bucket =>
      withBag(
        bucket,
        dataFileCount = dataFileCount,
        createDataManifest = createDataManifestWithExtraFile) { bagLocation =>
        withActorSystem { actorSystem =>
          implicit val ec = actorSystem.dispatcher
          withMaterializer { mat =>
            implicit val _mat = mat

            val service = new Verifier(
              storageManifestService = new StorageManifestService(),
              s3Client = s3Client,
              algorithm = MessageDigestAlgorithms.SHA_256
            )

            val future = service.verify(bagLocation)
            whenReady(future) { result =>
              result shouldBe a[IngestFailed[_]]

              val summary = result.summary

              summary.successfulVerifications should have size expectedDataFileCount
              summary.failedVerifications should have size 1

              val brokenFile = summary.failedVerifications.head
              brokenFile.reason shouldBe a[RuntimeException]
              brokenFile.reason.getMessage should startWith(
                "The specified key does not exist")
            }
          }
        }
      }
    }
  }

  it("fails a bag if the file manifest does not exist") {
    def dontCreateTheDataManifest(
      dataFiles: List[(String, String)]): Option[FileEntry] = None

    withLocalS3Bucket { bucket =>
      withBag(bucket, createDataManifest = dontCreateTheDataManifest) {
        bagLocation =>
          withActorSystem { actorSystem =>
            implicit val ec = actorSystem.dispatcher
            withMaterializer { mat =>
              implicit val _mat = mat

              val service = new Verifier(
                storageManifestService = new StorageManifestService(),
                s3Client = s3Client,
                algorithm = MessageDigestAlgorithms.SHA_256
              )

              val future = service.verify(bagLocation)

              whenReady(future) { result =>
                result shouldBe a[IngestFailed[_]]
                val err = result
                  .asInstanceOf[IngestFailed[VerificationSummary]]
                  .e

                err shouldBe a[RuntimeException]
                err.getMessage should startWith("Error getting file manifest")
              }
            }
          }
      }
    }
  }

  it("fails a bag if the tag manifest does not exist") {
    def dontCreateTheTagManifest(
      dataFiles: List[(String, String)]): Option[FileEntry] = None

    withLocalS3Bucket { bucket =>
      withBag(bucket, createTagManifest = dontCreateTheTagManifest) {
        bagLocation =>
          withActorSystem { actorSystem =>
            implicit val ec = actorSystem.dispatcher
            withMaterializer { mat =>
              implicit val _mat = mat

              val service = new Verifier(
                storageManifestService = new StorageManifestService(),
                s3Client = s3Client,
                algorithm = MessageDigestAlgorithms.SHA_256
              )

              val future = service.verify(bagLocation)

              whenReady(future) { result =>
                result shouldBe a[IngestFailed[_]]
                val err = result
                  .asInstanceOf[IngestFailed[VerificationSummary]]
                  .e

                err shouldBe a[RuntimeException]
                err.getMessage should startWith("Error getting tag manifest")
              }
            }
          }
      }
    }
  }
}
