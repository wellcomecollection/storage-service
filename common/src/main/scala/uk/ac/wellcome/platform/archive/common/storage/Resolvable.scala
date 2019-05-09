package uk.ac.wellcome.platform.archive.common.storage

import uk.ac.wellcome.storage.ObjectLocation

trait Resolvable[LocationResolvable <: Located] {
  def resolve(root: ObjectLocation)(locatable: LocationResolvable): ObjectLocation
}

object Resolvable {
  implicit def resolvable[LocationResolvable <: Located](
                                                                implicit locator: LocationResolvable => ObjectLocation
                                                              ) =
    new Resolvable[LocationResolvable] {
      override def resolve(root: ObjectLocation)(locatable: LocationResolvable): ObjectLocation =
        locator(locatable)
    }

  implicit class Resolver[LocationResolvable <: Located](locatable: LocationResolvable)(
    implicit resolver: Resolvable[LocationResolvable]
  ) {
    def resolve(root: ObjectLocation): ObjectLocation =
      resolver.resolve(root)(locatable)

  }
}