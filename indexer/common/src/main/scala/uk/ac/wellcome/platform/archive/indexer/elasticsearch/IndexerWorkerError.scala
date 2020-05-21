package uk.ac.wellcome.platform.archive.indexer.elasticsearch

sealed trait IndexerWorkerError extends Exception

case class RetryableIndexingError[T](payload: T, cause: Throwable)
    extends IndexerWorkerError {
  initCause(cause)
}

case class FatalIndexingError[T](payload: T) extends IndexerWorkerError
