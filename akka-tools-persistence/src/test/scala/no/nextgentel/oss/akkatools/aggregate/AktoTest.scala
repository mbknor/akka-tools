package no.nextgentel.oss.akkatools.aggregate

class AktoTest {

}

trait AktoTestEvent

trait AktoTestState extends AktoState[AktoTestEvent, AktoTestState]


case class AktoTestStateEmpty() extends AktoTestState {

  override def transition(event: AktoTestEvent): AktoTestState = ???

  override def generateDMs: PartialFunction[AktoTestEvent, ResultingDMs] = ???

  override def cmdToEvent: PartialFunction[AggregateCmd, ResultingEvent[AktoTestEvent]] = ???
}