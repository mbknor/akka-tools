package no.ngt.oss.akkatools.aggregate

import akka.actor._
import akka.contrib.pattern.ClusterSharding
import no.ngt.oss.akkatools.persistence.{NgtPersistentView, NgtPersistentActor, NgtPersistentShardingActor}

import scala.concurrent.duration.FiniteDuration
import scala.reflect._
case class AggregateError(errorMsg:String) extends RuntimeException(errorMsg)

trait AggregateState[E, T <: AggregateState[E,T]] {
  def transition(event:E):T

}

/**
 * Dispatcher - When sending something to an ES, use its dispatcher
 * Command - Dispatchable message - When sent to the dispatcher, it will be sent to the correct ES.
 *
 * Event - Represents a change of state for an ES
 *
 * State is immutable.
 *   Represents the full state of the entity. based on its state it can accept or reject an event.
 *   Has with method transition(event) - if ok, it returns new state. If not, an error is thrown.
 *
 *   Can be used to try an event (since it is mutable)
 *
 * DurableMessage: method of sending a message which with retry-mechanism until confirm() is called.
 *
 * GeneralAggregate pseudocode:
 *
 * for each received cmd:
 *    convert it to event
 *    try the event (by calling state.transition() )
 *      if it failed: maybe do something
 *      if it works:
 *        persist event
 *        generate and send ExternalEffect (DurableMessages that needs to be sent)
 *        change our current state (by calling state.transition() and keeping the result )
 *
 *
 */
abstract class GeneralAggregate[E:ClassTag, S <: AggregateState[E, S]:ClassTag]
(
  idleTimeout:FiniteDuration,
  ourDispatcherActor:ActorPath
  ) extends NgtPersistentShardingActor[E, AggregateError](idleTimeout, ourDispatcherActor){

  def this(ourDispatcherActor:ActorPath) = this(NgtPersistentActor.DEFAULT_IDLE_TIMEOUT_IN_SECONDS, ourDispatcherActor)

  var state:S

  case class ExternalEffect(message:AnyRef, destination:ActorPath)

  object ExternalEffects {
    def apply(message:AnyRef, destination:ActorPath):ExternalEffects = ExternalEffects(List(ExternalEffect(message, destination)))
  }

  case class ExternalEffects(list:List[ExternalEffect])

  private val defaultErrorHandler = (errorMsg:String) => log.debug("No cmdFailed-handler executed")
  private val defaultExternalEffectsHandler = (e:E) => {
    log.debug("No externalEffects handler for this event")
    ExternalEffects(List())
  }

  case class EventResult(event:E, errorHandler:(String)=>Unit = defaultErrorHandler)

  def cmdToEvent:PartialFunction[AnyRef, EventResult]
  def generateExternalEffects:PartialFunction[E, ExternalEffects]


  final override protected def stateInfo(): String = state.toString

  final def tryCommand = {
    case x:AnyRef =>
      // Can't get pattern-matching to work with generics..
      if (x.isInstanceOf[GetAggregateState]) {
        sender ! state
      } else {
        val cmd = x
        val defaultCmdToEvent:(AnyRef) => EventResult = {(q) => throw AggregateError("Do not know how to process cmd of type " + q.getClass)}
        val eventResult:EventResult = cmdToEvent.applyOrElse(cmd, defaultCmdToEvent)
        // Test the events
        try {
          // for now we only have one event, but act as we have a list..
          val events = List(eventResult.event)
          events.foldLeft(state) {
            (s, e) =>
              s.transition(e)
          }

          // it was valid - we can persist it
          persistAndApplyEvents(events)
        } catch {
          case error:AggregateError =>
            eventResult.errorHandler.apply(error.errorMsg)
            throw error
        }
      }
  }


  final def onEvent = {
    case e:E =>
      val newState = state.transition(e)
      val externalEffects = generateExternalEffects.applyOrElse(e, defaultExternalEffectsHandler)
      state = newState
      externalEffects.list.foreach {
        externalEffect =>
          sendAsDurableMessage(externalEffect.message, externalEffect.destination)
      }
  }

}


class GeneralAggregateView[E:ClassTag, S <: AggregateState[E, S]:ClassTag]
(
  persistenceIdBase:String,
  id:String,
  initialState:S,
  collectHistory:Boolean = true
  ) extends NgtPersistentView[E, S](persistenceIdBase, id, collectHistory) {

  var state:S = initialState

  override def currentState():S = state

  override def applyEventToState(event: E): Unit = {
    state = state.transition(event)
  }

  override val onCmd: PartialFunction[AnyRef, Unit] = {
    case x:GetAggregateState =>
      sender ! state
  }
}