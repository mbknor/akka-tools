package no.nextgentel.oss.akkatools.example2.trustaccountcreation

import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestProbe, TestKit}
import com.typesafe.config.ConfigFactory
import no.nextgentel.oss.akkatools.example2.emailsystem.DoSendEmailToCustomer
import no.nextgentel.oss.akkatools.example2.esigningsystem.DoPerformESigning
import no.nextgentel.oss.akkatools.example2.processing.DoCreateTrustAccount
import no.nextgentel.oss.akkatools.persistence.DurableMessageForwardAndConfirm
import no.nextgentel.oss.akkatools.testing.AggregateStateGetter
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, FunSuiteLike}
import org.slf4j.LoggerFactory
import no.nextgentel.oss.akkatools.testing.DurableMessageTesting._
import StateName._

class TACAggregateTest (_system:ActorSystem) extends TestKit(_system) with FunSuiteLike with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  def this() = this(ActorSystem("tac-test-actor-system", ConfigFactory.load("application-test.conf")))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val log = LoggerFactory.getLogger(getClass)
  private def generateId() = UUID.randomUUID().toString

  var id:String=null
  var ourDispatcher:TestProbe=null
  var eSigningSystem:TestProbe=null
  var emailSystem:TestProbe=null
  var trustAccountSystem:TestProbe = null
  var main:ActorRef=null
  var stateGetter:AggregateStateGetter[TACState]=null


  before {
    id = generateId()
    ourDispatcher      = TestProbe()
    eSigningSystem     = TestProbe()
    emailSystem        = TestProbe()
    trustAccountSystem = TestProbe()
    main = system.actorOf(TACAggregate.props(ourDispatcher.ref.path, DurableMessageForwardAndConfirm(eSigningSystem.ref).path, DurableMessageForwardAndConfirm(emailSystem.ref).path, DurableMessageForwardAndConfirm(trustAccountSystem.ref).path), "TACAggregate-" + id)
    stateGetter = AggregateStateGetter[TACState](main)
  }


  def assertState(correctState:TACState): Unit = {
    assert(stateGetter.getState() == correctState)
  }

  test("Normal flow") {

    // Make sure we start with empty state
    assertState(TACState.empty())

    val info = TrustAccountCreationInfo("Customer-1", "type-X")

    // start the trustAccountCreation-process
    sendDMBlocking(main, CreateNewTACCmd(id, info))
    assertState( TACState(PENDING_E_SIGNING, Some(info), None, None) )

    // We must make sure that our e-signing-system has been told that e-signing should start
    eSigningSystem.expectMsg(DoPerformESigning(info.customerNo))

    // Fake the completion of the e-signing
    sendDMBlocking(main, ESigningCompletedCmd(id))
    assertState( TACState(PROCESSING, Some(info), None, None) )

    // make sure TrustAccountProcessingSystem is told to process it
    trustAccountSystem.expectMsg(DoCreateTrustAccount(info.customerNo, info.trustAccountType))

    // Fake creation of the trustAccount
    val trustAccountId = "TA-1"
    sendDMBlocking(main, TACCompletedCmd(id, trustAccountId))
    assertState( TACState(CREATED, Some(info), Some(trustAccountId), None) )

    // make sure the customer is emailed
    emailSystem.expectMsg(DoSendEmailToCustomer(info.customerNo, s"Your TrustAccount '$trustAccountId' has been created!"))

  }
}
