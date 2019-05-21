package uk.ac.wellcome.platform.archive.common.storage

import java.net.URI

trait Resolvable[T] {
  def resolve(t: T): URI
}

object Resolvable {
  implicit class ResolverOps[T](t: T)(
    implicit resolver: Resolvable[T]
  ) {
    def resolve: URI =
      resolver.resolve(t)
  }
}

