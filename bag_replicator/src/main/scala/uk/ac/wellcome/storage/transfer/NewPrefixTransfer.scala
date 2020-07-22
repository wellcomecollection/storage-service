package uk.ac.wellcome.storage.transfer

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.{Location, Prefix}
import uk.ac.wellcome.storage.listing.Listing

import scala.collection.parallel.ParIterable

trait NewPrefixTransfer[SrcLocation <: Location, SrcPrefix <: Prefix[
  SrcLocation
], DstLocation <: Location, DstPrefix <: Prefix[DstLocation]]
    extends Logging {
  implicit val transfer: NewTransfer[SrcLocation, DstLocation]
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
    checkForExisting: Boolean
  ): Either[PrefixTransferFailure, PrefixTransferSuccess] = {
    var successes = 0
    var failures = 0

    iterator
      .grouped(10)
      .foreach { locations =>
        val results: ParIterable[
          (SrcLocation, Either[NewTransferFailure, NewTransferSuccess])
        ] =
          locations.par.map { srcLocation =>
            (
              srcLocation,
              transfer.transfer(
                src = srcLocation,
                dst = buildDstLocation(
                  srcPrefix = srcPrefix,
                  dstPrefix = dstPrefix,
                  srcLocation = srcLocation
                ),
                checkForExisting = checkForExisting
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
      left = PrefixTransferFailure(failures, successes)
    )
  }

  def transferPrefix(
    srcPrefix: SrcPrefix,
    dstPrefix: DstPrefix,
    checkForExisting: Boolean = true
  ): Either[TransferFailure, PrefixTransferSuccess] = {
    listing.list(srcPrefix) match {
      case Left(error) =>
        Left(PrefixTransferListingFailure(srcPrefix, error.e))

      case Right(iterable) =>
        copyPrefix(
          iterator = iterable,
          srcPrefix = srcPrefix,
          dstPrefix = dstPrefix,
          checkForExisting = checkForExisting
        )
    }
  }
}
