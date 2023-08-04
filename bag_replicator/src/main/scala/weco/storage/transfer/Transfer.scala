package weco.storage.transfer

trait Transfer[SrcLocation, DstLocation] {
  type FailureResult = TransferFailure[SrcLocation, DstLocation]
  type SuccessResult = TransferSuccess[SrcLocation, DstLocation]

  type TransferEither = Either[FailureResult, SuccessResult]

  // Note: implementations are expected to check whether there is already
  // an object at `dst`, and if so:
  //
  //    - Return a TransferNoOp if it matches the source object
  //    - Return a TransferOverwriteFailure if it is different to the source object
  //
  // We used to skip checking for an existing object if we knew there was nothing
  // there (and to avoid consistency issues with S3), but in August 2022 we switched
  // to always checking for a destination object first.
  //
  // See https://github.com/wellcomecollection/platform/issues/3897#issuecomment-1201264674
  def transfer(src: SrcLocation, dst: DstLocation): TransferEither
}
