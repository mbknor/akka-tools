package no.nextgentel.oss.akkatools.persistence

import akka.actor.{ActorRef, ActorRefFactory}

/**
 * DurableMessage is implemented in akka-tools-persistence
 *
 * This trait, DurableMessageLike, is only here to allow other akka-tools modules to interact with
 * DurableMessage without needing to drag lots of dependencies with it.
 *
 */
trait DurableMessageLike {
  def payload():AnyRef
  def withNewPayload(newPayload:AnyRef):DurableMessageLike
  def confirm(systemOrContext:ActorRefFactory, confirmingActor:ActorRef):Unit
}
