package no.nextgentel.oss.akkatools.aggregate;

public class JavaState implements AggregateStateJava {

    public static JavaState empty = new JavaState(0);

    private final int counter;

    public JavaState(int counter) {
        this.counter = counter;
    }

    @Override
    public JavaState transition(Object event) {
        if ( event instanceof IncrementEvent ) {
            return new JavaState(counter + 1);
        } else {
            throw new JavaError("Invalid command");
        }
    }
}
