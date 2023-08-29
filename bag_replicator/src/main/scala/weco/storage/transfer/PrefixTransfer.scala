package weco.storage.transfer

import grizzled.slf4j.Logging
import weco.storage.listing.Listing

import scala.collection.parallel.ParIterable

trait PrefixTransfer[SrcPrefix, SrcLocation, DstPrefix, DstLocation]
    extends Logging {
  implicit val transfer: Transfer[SrcLocation, DstLocation]
  implicit val listing: Listing[SrcPrefix, SrcLocation]

  protected def buildDstLocation(
    srcPrefix: SrcPrefix,
    dstPrefix: DstPrefix,
    srcLocation: SrcLocation
  ): DstLocation

  private def copyPrefix(
    iterator: Iterable[SrcLocation],
    srcPrefix: SrcPrefix,
    dstPrefix: DstPrefix,
  ): Either[PrefixTransferIncomplete, PrefixTransferSuccess] = {
    var successes = 0
    var failures = 0

    iterator
      .grouped(10)
      .foreach { locations =>
        val results: ParIterable[(SrcLocation, transfer.TransferEither)] =
          locations.par.map { srcLocation =>
            (
              srcLocation,
              transfer.transfer(
                src = srcLocation,
                dst = buildDstLocation(
                  srcPrefix = srcPrefix,
                  dstPrefix = dstPrefix,
                  srcLocation = srcLocation
                )
              )
            )
          }

        results.foreach {
          case (srcLocation, Right(_)) =>
            debug(s"Successfully copied $srcLocation to $dstPrefix")
            successes += 1
          case (srcLocation, Left(err)) =>
            warn(s"Error copying $srcLocation to $dstPrefix: $err")
            failures += 1
        }
      }

    Either.cond(
      test = failures == 0,
      right = PrefixTransferSuccess(successes),
      left = PrefixTransferIncomplete(failures, successes)
    )
  }

  def transferPrefix(
    srcPrefix: SrcPrefix,
    dstPrefix: DstPrefix
  ): Either[PrefixTransferFailure, PrefixTransferSuccess] = {
    listing.list(srcPrefix) match {
      case Left(error) =>
        Left(PrefixTransferListingFailure(srcPrefix, error))

      case Right(iterable) =>
        copyPrefix(
          iterator = iterable,
          srcPrefix = srcPrefix,
          dstPrefix = dstPrefix
        )
    }
  }
}
