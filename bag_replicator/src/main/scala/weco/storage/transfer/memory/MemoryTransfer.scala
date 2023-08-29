package weco.storage.transfer.memory

import weco.storage.store.memory.MemoryStoreBase
import weco.storage.transfer._

trait MemoryTransfer[Ident, T]
    extends Transfer[Ident, Ident]
    with MemoryStoreBase[Ident, T] {
  override def transfer(src: Ident, dst: Ident): TransferEither =
    (entries.get(src), entries.get(dst)) match {
      case (Some(srcT), Some(dstT)) if srcT == dstT =>
        Right(TransferNoOp(src, dst))
      case (Some(_), Some(_)) =>
        Left(TransferOverwriteFailure(src, dst))
      case (Some(srcT), _) =>
        entries = entries ++ Map(dst -> srcT)
        Right(TransferPerformed(src, dst))
      case (None, _) =>
        Left(TransferSourceFailure(src, dst))
    }
}
