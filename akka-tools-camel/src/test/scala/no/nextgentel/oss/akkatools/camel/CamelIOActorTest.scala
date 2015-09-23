package no.nextgentel.oss.akkatools.camel

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.camel.{Ack, Consumer, CamelMessage, CamelExtension}
import akka.testkit.{TestProbe, TestKit}
import no.nextgentel.oss.akkatools.persistence.DurableMessageLike
import org.apache.activemq.camel.component.ActiveMQComponent
import org.scalatest.{FunSuiteLike, Matchers, BeforeAndAfter, BeforeAndAfterAll}


class CamelIOActorTest(_system:ActorSystem) extends TestKit(_system) with FunSuiteLike with Matchers with BeforeAndAfterAll with BeforeAndAfter {
  def this() = this(ActorSystem("CamelIOActorTest"))

  val activeMQBrokerUrl = "vm://localhost?broker.persistent=false"
  val camel = CamelExtension(system)
  val camelContext = camel.context

  camelContext.addComponent("activemq", ActiveMQComponent.activeMQComponent(activeMQBrokerUrl))

  val plainChannelDirection1aa = CamelChannelConfig("activemq:queue:plainChannel-1-aa", "activemq:queue:plainChannel-2-aa")
  val plainChannelDirection2aa = CamelChannelConfig(plainChannelDirection1aa.outboundEndpoint, plainChannelDirection1aa.inboundEndpoint)

  val plainDest1aa = TestProbe()
  val plainDest2aa = TestProbe()

  val plain1aa = system.actorOf(Props(new OurCamelIOActor(plainChannelDirection1aa, plainDest1aa.ref, true)))
  val plain2aa = system.actorOf(Props(new OurCamelIOActor(plainChannelDirection2aa, plainDest2aa.ref, true)))

  val plainChannelDirection1ma = CamelChannelConfig("activemq:queue:plainChannel-1-ma", "activemq:queue:plainChannel-2-ma")
  val plainChannelDirection2ma = CamelChannelConfig(plainChannelDirection1ma.outboundEndpoint, plainChannelDirection1ma.inboundEndpoint)

  val plainDest1ma = TestProbe()
  val plainDest2ma = TestProbe()

  val plain1ma = system.actorOf(Props(new OurCamelIOActor(plainChannelDirection1ma, createReceiverThatFailsFirst(plainDest1ma.ref), false)))
  val plain2ma = system.actorOf(Props(new OurCamelIOActor(plainChannelDirection2ma, createReceiverThatFailsFirst(plainDest2ma.ref), false)))

  val sender = TestProbe()



  val msgId = new AtomicInteger(0)
  def generateNewMessage(): String = {
    "msg-" + msgId.incrementAndGet()
  }

  def createReceiverThatFailsFirst(dest:ActorRef):ActorRef = {
    system.actorOf(Props(new ReceiverThatFailsFirst(dest)))
  }

  test("plain sending and receiving - no dm - auto-acking") {

    var msg = generateNewMessage()
    plain1aa ! msg
    plainDest2aa.expectMsg(msg)

    msg = generateNewMessage()
    plain2aa ! msg
    plainDest1aa.expectMsg(msg)
  }

  test("plain sending and receiving - no dm - no auto-acking") {
    ???
  }

  test("plain sending and receiving - dm - auto-acking") {
    var msg = OurDM(generateNewMessage(), sender.ref)
    plain1aa ! msg
    plainDest2aa.expectMsg(msg.payload)
    sender.expectMsg("confirmed:"+msg.payload)

    msg = OurDM(generateNewMessage(), sender.ref)
    plain2aa ! msg
    plainDest1aa.expectMsg(msg.payload)
    sender.expectMsg("confirmed:"+msg.payload)

  }

  test("plain sending and receiving - dm - no auto-acking") {
    var msg = OurDM(generateNewMessage(), sender.ref)
    plain1ma ! msg
    plainDest2ma.expectMsg(msg.payload)
    sender.expectMsg("confirmed:"+msg.payload)

    msg = OurDM(generateNewMessage(), sender.ref)
    plain2ma ! msg
    plainDest1ma.expectMsg(msg.payload)
    sender.expectMsg("confirmed:"+msg.payload)

  }

  test("deserialize errorhandler") {
    ???
  }

  test("redelivering and none-acking") {
    ???
  }



  override protected def afterAll(): Unit = {
    shutdown(system)
  }

  case class OurDM(payload:String, confirmDest:ActorRef) extends DurableMessageLike {

    override def confirm(systemOrContext: ActorRefFactory, confirmingActor: ActorRef): Unit = {
      confirmDest ! "confirmed:"+payload
    }

    override def withNewPayload(newPayload: AnyRef): DurableMessageLike = ???
  }

  class OurCamelIOActor(channel: CamelChannelConfig, messageDest:ActorRef, autoAckWhenReceiving:Boolean) extends CamelIOActor(channel, messageDest, autoAckWhenReceiving) with Matchers {
    val prefix = "[SERIALIZED]"
    override def deserialize(camelMessage: CamelMessage): Any = {
      val raw = camelMessage.bodyAs[String]
      assert(raw.startsWith(prefix))
      raw.substring(prefix.length)
    }

    override def serialize(message: Any): Any = {
      val s = message.asInstanceOf[String]
      prefix + s
    }
  }


  class ReceiverThatFailsFirst(consumer:ActorRef) extends Actor {

    var shouldFail = true

    override def receive: Receive = {
      case msg:String =>

        if(shouldFail) {
          println("Faking failing by not doing anything")
          shouldFail = false
        } else {
          sender ! Ack
          consumer ! msg
        }
    }
  }




}
