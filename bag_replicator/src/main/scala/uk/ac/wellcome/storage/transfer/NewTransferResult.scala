package uk.ac.wellcome.storage.transfer

sealed trait NewTransferResult

sealed trait NewTransferFailure extends NewTransferResult {
  val e: Throwable
}

case class NewTransferSourceFailure[SrcLocation, DstLocation](
  source: SrcLocation,
  destination: DstLocation,
  e: Throwable = new Error()
) extends NewTransferFailure

case class NewTransferDestinationFailure[SrcLocation, DstLocation](
  source: SrcLocation,
  destination: DstLocation,
  e: Throwable = new Error()
) extends NewTransferFailure

case class NewTransferOverwriteFailure[SrcLocation, DstLocation](
  source: SrcLocation,
  destination: DstLocation,
  e: Throwable = new Error()
) extends NewTransferFailure

case class NewPrefixTransferFailure(
  failures: Int,
  successes: Int,
  e: Throwable = new Error()
) extends NewTransferFailure

case class NewPrefixTransferListingFailure[Prefix](
  prefix: Prefix,
  e: Throwable = new Error()
) extends NewTransferFailure

sealed trait NewTransferSuccess extends NewTransferResult

case class NewTransferNoOp[SrcLocation, DstLocation](
  source: SrcLocation,
  destination: DstLocation
) extends NewTransferSuccess

case class NewTransferPerformed[SrcLocation, DstLocation](
  source: SrcLocation,
  destination: DstLocation
) extends NewTransferSuccess

case class NewPrefixTransferSuccess(successes: Int) extends NewTransferSuccess
