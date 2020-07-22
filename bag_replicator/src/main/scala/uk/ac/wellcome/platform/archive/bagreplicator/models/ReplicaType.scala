package uk.ac.wellcome.platform.archive.bagreplicator.models

// We distinguish between primary and secondary replicas.
//
//  - The primary replica is the "warm" copy, intended for frequent access
//    e.g. an S3 bucket with Standard-IA
//
//  - Secondary replicas are the "cold" copies, that give us an extra layer
//    of backup but aren't intended for frequent access
//    e.g. an S3 Glacier bucket

sealed trait ReplicaType

case object PrimaryReplica extends ReplicaType
case object SecondaryReplica extends ReplicaType
