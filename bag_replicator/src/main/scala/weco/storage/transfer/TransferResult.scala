package weco.storage.transfer

sealed trait TransferResult[SrcLocation, DstLocation] {
  val src: SrcLocation
  val dst: DstLocation
}

sealed trait TransferFailure[SrcLocation, DstLocation]
    extends TransferResult[SrcLocation, DstLocation] {
  val e: Throwable
}

case class TransferSourceFailure[SrcLocation, DstLocation](src: SrcLocation,
                                                           dst: DstLocation,
                                                           e: Throwable =
                                                             new Error())
    extends TransferFailure[SrcLocation, DstLocation]

case class TransferDestinationFailure[SrcLocation, DstLocation](
  src: SrcLocation,
  dst: DstLocation,
  e: Throwable = new Error())
    extends TransferFailure[SrcLocation, DstLocation]

case class TransferOverwriteFailure[SrcLocation, DstLocation](src: SrcLocation,
                                                              dst: DstLocation,
                                                              e: Throwable =
                                                                new Error())
    extends TransferFailure[SrcLocation, DstLocation]

sealed trait TransferSuccess[SrcLocation, DstLocation]
    extends TransferResult[SrcLocation, DstLocation]

case class TransferNoOp[SrcLocation, DstLocation](src: SrcLocation,
                                                  dst: DstLocation)
    extends TransferSuccess[SrcLocation, DstLocation]

case class TransferPerformed[SrcLocation, DstLocation](src: SrcLocation,
                                                       dst: DstLocation)
    extends TransferSuccess[SrcLocation, DstLocation]
