package no.nextgentel.oss.akkatools.aggregate;


import akka.actor.ActorPath;

public class MyAggregate extends GeneralAggregateJava {

    public MyAggregate(ActorPath ourDispatcherActor) {
        super(JavaState.empty, ourDispatcherActor);
    }


    @Override
    public ResultingEvent<Object> onCmdToEvent(Object cmd) {
        return null;
    }

    @Override
    public ResultingDurableMessages onGenerateResultingDurableMessages(Object event) {
        return null;
    }
}
