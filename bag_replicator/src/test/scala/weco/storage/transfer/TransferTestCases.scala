package weco.storage.transfer

import org.scalatest.EitherValues
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import weco.fixtures.TestWith
import weco.storage.store.Store

trait TransferTestCases[SrcLocation,
                        DstLocation,
                        T,
                        SrcNamespace,
                        DstNamespace,
                        SrcStore <: Store[SrcLocation, T],
                        DstStore <: Store[DstLocation, T],
                        Context]
    extends AnyFunSpec
    with Matchers
    with EitherValues {

  def createT: T

  def withContext[R](testWith: TestWith[Context, R]): R

  def withSrcNamespace[R](testWith: TestWith[SrcNamespace, R]): R
  def withDstNamespace[R](testWith: TestWith[DstNamespace, R]): R

  def withNamespacePair[R](
    testWith: TestWith[(SrcNamespace, DstNamespace), R]): R =
    withSrcNamespace { srcNamespace =>
      withDstNamespace { dstNamespace =>
        testWith((srcNamespace, dstNamespace))
      }
    }

  def createSrcLocation(namespace: SrcNamespace): SrcLocation
  def createDstLocation(namespace: DstNamespace): DstLocation

  def createSrcObject(namespace: SrcNamespace): (SrcLocation, T) =
    (createSrcLocation(namespace), createT)

  def withSrcStore[R](initialEntries: Map[SrcLocation, T])(
    testWith: TestWith[SrcStore, R])(implicit context: Context): R
  def withDstStore[R](initialEntries: Map[DstLocation, T])(
    testWith: TestWith[DstStore, R])(implicit context: Context): R

  def withTransfer[R](srcStore: SrcStore, dstStore: DstStore)(
    testWith: TestWith[Transfer[SrcLocation, DstLocation], R]): R

  describe("behaves as a Transfer") {
    it("copies an object from a source to a destination") {
      withNamespacePair {
        case (srcNamespace, dstNamespace) =>
          val (src, t) = createSrcObject(srcNamespace)
          val dst = createDstLocation(dstNamespace)

          withContext { implicit context =>
            withSrcStore(initialEntries = Map(src -> t)) { srcStore =>
              withDstStore(initialEntries = Map.empty) { dstStore =>
                val result =
                  withTransfer(srcStore, dstStore) {
                    _.transfer(src, dst)
                  }

                result.value shouldBe TransferPerformed(src, dst)

                srcStore.get(src).value.identifiedT shouldBe t
                dstStore.get(dst).value.identifiedT shouldBe t
              }
            }
          }
      }
    }

    it("errors if the source does not exist") {
      withNamespacePair {
        case (srcNamespace, dstNamespace) =>
          val src = createSrcLocation(srcNamespace)
          val dst = createDstLocation(dstNamespace)

          withContext { implicit context =>
            withSrcStore(initialEntries = Map.empty) { srcStore =>
              withDstStore(initialEntries = Map.empty) { dstStore =>
                val result =
                  withTransfer(srcStore, dstStore) {
                    _.transfer(src, dst)
                  }

                result.left.value shouldBe a[TransferSourceFailure[_, _]]

                val failure = result.left.value
                  .asInstanceOf[TransferSourceFailure[SrcLocation, DstLocation]]
                failure.src shouldBe src
                failure.dst shouldBe dst
              }
            }
          }
      }
    }

    it("errors if the source and destination both exist and are different") {
      withNamespacePair {
        case (srcNamespace, dstNamespace) =>
          val src = createSrcLocation(srcNamespace)
          val dst = createDstLocation(dstNamespace)

          val srcT = createT
          val dstT = createT

          withContext { implicit context =>
            withSrcStore(initialEntries = Map(src -> srcT)) { srcStore =>
              withDstStore(initialEntries = Map(dst -> dstT)) { dstStore =>
                val result =
                  withTransfer(srcStore, dstStore) {
                    _.transfer(src, dst)
                  }

                result.left.value shouldBe a[TransferOverwriteFailure[_, _]]

                result.left.value.src shouldBe src
                result.left.value.dst shouldBe dst

                srcStore.get(src).value.identifiedT shouldBe srcT
                dstStore.get(dst).value.identifiedT shouldBe dstT
              }
            }
          }
      }
    }

    it(
      "allows a no-op copy if the source and destination both exist and are the same") {
      withNamespacePair {
        case (srcNamespace, dstNamespace) =>
          val src = createSrcLocation(srcNamespace)
          val dst = createDstLocation(dstNamespace)

          val t = createT

          withContext { implicit context =>
            withSrcStore(initialEntries = Map(src -> t)) { srcStore =>
              withDstStore(initialEntries = Map(dst -> t)) { dstStore =>
                val result =
                  withTransfer(srcStore, dstStore) {
                    _.transfer(src, dst)
                  }

                result.value shouldBe TransferNoOp(src, dst)
                srcStore.get(src).value.identifiedT shouldBe t
                dstStore.get(dst).value.identifiedT shouldBe t
              }
            }
          }
      }
    }
  }
}
