package no.nextgentel.oss.akkatools.aggregate

import akka.actor.ActorPath

import scala.reflect.ClassTag


trait AktoStateBase[E, T <: AktoStateBase[E,T]] extends AggregateState[E, T] {

  def cmdToEvent:PartialFunction[AggregateCmd, ResultingEvent[E]]

  // Called AFTER event has been applied to state
  def generateDMs(event:E, previousState:T):ResultingDMs
}

trait AktoState[E, T <: AktoState[E,T]] extends AktoStateBase[E, T] {

  // Called AFTER event has been applied to state
  override def generateDMs(event: E, previousState: T): ResultingDMs = {
    generateDMs.applyOrElse(event, (t:E) => ResultingDMs(List()))
  }

  // Override it if you want to generate DMs
  def generateDMs:PartialFunction[E, ResultingDMs] = Map.empty

}

abstract class AktoAggregate[E:ClassTag, S <: AktoStateBase[E, S]:ClassTag](dmSelf:ActorPath)
  extends GeneralAggregateBase[E, S](dmSelf) {

  override def cmdToEvent: PartialFunction[AggregateCmd, ResultingEvent[E]] = {
    case cmd:AggregateCmd =>
      val defaultCmdToEvent:(AggregateCmd) => ResultingEvent[E] = {(q) => ResultingEvent(List[E]())} // Do nothing..
      state.cmdToEvent.applyOrElse(cmd, defaultCmdToEvent)
  }

  // Called AFTER event has been applied to state
  override def generateDMs(event: E, previousState: S): ResultingDMs = {
    state.generateDMs(event, previousState)
  }

}