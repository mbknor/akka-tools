package no.nextgentel.oss.akkatools.example2.trustaccountcreation

import java.util.concurrent.TimeUnit

import akka.actor.{Props, ActorPath}
import no.nextgentel.oss.akkatools.aggregate.{ResultingDurableMessages, ResultingEvent, AggregateCmd, GeneralAggregate}
import no.nextgentel.oss.akkatools.example2.emailsystem.DoSendEmailToCustomer
import no.nextgentel.oss.akkatools.example2.esigningsystem.DoPerformESigning
import no.nextgentel.oss.akkatools.example2.processing.DoCreateTrustAccount
import no.nextgentel.oss.akkatools.persistence.SendAsDurableMessage

import scala.concurrent.duration.FiniteDuration

class TACAggregate
(
  ourDispatcher:ActorPath,
  eSigningSystem:ActorPath,
  emailSystem:ActorPath,
  trustAccountSystem:ActorPath
) extends GeneralAggregate[TACEvent, TACState](FiniteDuration(60, TimeUnit.SECONDS), ourDispatcher) {


  override var state = TACState.empty() // This is the state of our initial state (empty)

  // transform command to event
  override def cmdToEvent = {
    case c:CreateNewTACCmd        => new ResultingEvent( List(RegisteredEvent(c.info), ESigningStartedEvent()) )
    case c:ESigningFailedCmd      => ResultingEvent( ESigningFailedEvent() )
    case c:ESigningCompletedCmd   => ResultingEvent( ESigningCompletedEvent() )
    case c:TACCompletedCmd        => ResultingEvent( CreatedEvent(c.trustAccountId) )
    case c:TACDeclined            => ResultingEvent( DeclinedEvent(c.cause) )
  }

  override def generateResultingDurableMessages = {
    case e:ESigningStartedEvent  =>
      // We must send message to eSigningSystem
      val msg = DoPerformESigning(state.info.get.customerNo)
      ResultingDurableMessages( msg, eSigningSystem)

    case e:DeclinedEvent =>
      // The TrustAccountCreation-process failed - must notify customer
      val msg = DoSendEmailToCustomer(state.info.get.customerNo, s"Sorry.. TAC-failed: ${e.cause}")
      ResultingDurableMessages(msg, emailSystem)

    case e:CreatedEvent =>
      // The TrustAccountCreation-process was success - must notify customer
      val msg = DoSendEmailToCustomer(state.info.get.customerNo, s"Your TrustAccount '${e.trustAccountId}' has been created!")
      ResultingDurableMessages(msg, emailSystem)

    case e:ESigningCompletedEvent =>
      // ESigning is completed, so we should init creation of the TrustAccount
      val info = state.info.get
      val msg = DoCreateTrustAccount(info.customerNo, info.trustAccountType)
      ResultingDurableMessages(msg, trustAccountSystem)
  }
}

object TACAggregate {

  def props(ourDispatcher:ActorPath,
            eSigningSystem:ActorPath,
            emailSystem:ActorPath,
            trustAccountSystem:ActorPath) = Props(new TACAggregate(ourDispatcher, eSigningSystem, emailSystem ,trustAccountSystem))
}
