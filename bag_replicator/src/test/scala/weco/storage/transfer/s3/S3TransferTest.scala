package weco.storage.transfer.s3

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.transfer.s3.S3TransferManager
import software.amazon.awssdk.transfer.s3.model.CopyRequest
import weco.fixtures.TestWith
import weco.storage.fixtures.S3Fixtures.Bucket
import weco.storage.generators.{Record, RecordGenerators}
import weco.storage.s3.S3ObjectLocation
import weco.storage.store.s3.{S3StreamStore, S3TypedStore}
import weco.storage.tags.s3.S3Tags
import weco.storage.transfer.{Transfer, TransferSourceFailure, TransferTestCases, _}

class S3TransferTest
    extends TransferTestCases[
      S3ObjectLocation,
      S3ObjectLocation,
      Record,
      Bucket,
      Bucket,
      S3TypedStore[Record],
      S3TypedStore[Record],
      Unit
    ]
    with S3TransferFixtures[Record]
    with RecordGenerators
    with MockitoSugar {

  override def withTransfer[R](srcStore: S3TypedStore[Record],
                               dstStore: S3TypedStore[Record])(
    testWith: TestWith[Transfer[S3ObjectLocation, S3ObjectLocation], R]): R =
    testWith(S3Transfer.apply)

  override def createT: Record = createRecord

  override def withContext[R](testWith: TestWith[Unit, R]): R =
    testWith(())

  val s3ServerException =
    S3Exception.builder()
      .message("We encountered an internal error. Please try again.")
      .statusCode(500)
      .build()

  // This test is intended to spot warnings from the SDK if we don't close
  // the dst inputStream correctly.
  it("errors if the destination exists but the source does not") {
    withLocalS3Bucket { bucket =>
      val src = createS3ObjectLocationWith(bucket)
      val dst = createS3ObjectLocationWith(bucket)

      val initialEntries = Map(dst -> createRecord)

      withTransferStore(initialEntries) { implicit store =>
        withTransfer { transfer =>
          transfer
            .transfer(src, dst)
            .left
            .value shouldBe a[TransferSourceFailure[_, _]]
        }
      }
    }
  }

  it("doesn't replicate tags") {
    withLocalS3Bucket { bucket =>
      val src = createS3ObjectLocationWith(bucket)
      val dst = createS3ObjectLocationWith(bucket)

      val initialEntries = Map(src -> createRecord)

      val s3Tags = new S3Tags()

      withTransferStore(initialEntries) { implicit store =>
        s3Tags
          .update(src) { _ =>
            Right(Map("srcTag" -> "srcValue"))
          } shouldBe a[Right[_, _]]

        withTransfer { transfer =>
          transfer.transfer(src, dst) shouldBe a[Right[_, _]]
        }

        s3Tags.get(src).value.identifiedT shouldBe Map("srcTag" -> "srcValue")
        s3Tags.get(dst).value.identifiedT shouldBe empty
      }
    }
  }

  it("allows a no-op copy if the source and destination are the same") {
    withLocalS3Bucket { bucket =>
      val src = createS3ObjectLocationWith(bucket)
      val t = createT

      withTransferStore(initialEntries = Map(src -> t)) { implicit store =>
        val result =
          withTransfer {
            _.transfer(src, src)
          }

        result.value shouldBe TransferNoOp(src, src)

        store.get(src).value.identifiedT shouldBe t
      }
    }
  }

  it("retries 500 errors from S3 when calling copy on TransferManager") {
    withNamespacePair {
      case (srcNamespace, dstNamespace) =>
        val src = createSrcLocation(srcNamespace)
        val dst = createDstLocation(dstNamespace)

        val t = createT

        withContext { implicit context =>
          withSrcStore(initialEntries = Map(src -> t)) { srcStore =>
            withDstStore(initialEntries = Map.empty) { dstStore =>
              val failingOnceTransfer = mock[S3TransferManager]
              when(failingOnceTransfer.copy(any[CopyRequest]))
                .thenThrow(s3ServerException)
                .thenAnswer((invocation: InvocationOnMock) => {
                  s3TransferManager.copy(
                    invocation.getArguments.toList.head.asInstanceOf[CopyRequest])
                })
              val transfer =
                new S3Transfer()(failingOnceTransfer, new S3StreamStore())

              val result = transfer.transfer(src, dst)

              result.value shouldBe TransferPerformed(src, dst)

              srcStore.get(src).value.identifiedT shouldBe t
              dstStore.get(dst).value.identifiedT shouldBe t
            }
          }
        }
    }
  }

  it("stops retrying if TransferManager fails more than 3 times") {
    withNamespacePair {
      case (srcNamespace, dstNamespace) =>
        val src = createSrcLocation(srcNamespace)
        val dst = createDstLocation(dstNamespace)

        val t = createT

        withContext { implicit context =>
          withSrcStore(initialEntries = Map(src -> t)) { _ =>
            withDstStore(initialEntries = Map.empty) { _ =>
              val alwaysFailingTransfer = mock[S3TransferManager]
              when(alwaysFailingTransfer.copy(any[CopyRequest]))
                .thenThrow(s3ServerException)
              val transfer =
                new S3Transfer()(alwaysFailingTransfer, new S3StreamStore())

              val result = transfer.transfer(src, dst)

              result.left.value shouldBe a[TransferSourceFailure[_, _]]
              verify(alwaysFailingTransfer, times(3))
                .copy(any[CopyRequest])
            }
          }
        }
    }
  }
}
