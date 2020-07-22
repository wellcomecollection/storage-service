package uk.ac.wellcome.storage.transfer

import uk.ac.wellcome.storage.{Location, ObjectLocation}

trait NewTransfer[SrcLocation <: Location, DstLocation <: Location] {
  protected val underlying: Transfer[ObjectLocation]

  def transfer(src: SrcLocation,
               dst: DstLocation,
               checkForExisting: Boolean = true)
  : Either[NewTransferFailure, NewTransferSuccess] =
    underlying.transfer(src.toObjectLocation, dst.toObjectLocation) match {
      case Right(TransferNoOp(_, _))                 => Right(NewTransferNoOp(src, dst))
      case Right(TransferPerformed(_, _))            => Right(NewTransferPerformed(src, dst))
      case Left(TransferSourceFailure(_, _, e))      => Left(NewTransferSourceFailure(src, dst, e))
      case Left(TransferDestinationFailure(_, _, e)) => Left(NewTransferDestinationFailure(src, dst, e))
      case Left(TransferOverwriteFailure(_, _, e))   => Left(NewTransferOverwriteFailure(src, dst, e))
    }
}
