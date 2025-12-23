package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.patterns.Reaper;
import de.ddm.actors.profiling.DataProvider;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.NoArgsConstructor;

public class Worker extends AbstractBehavior<Worker.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable {
    }

    @NoArgsConstructor
    public static class ShutdownMessage implements Message {
        private static final long serialVersionUID = 7516129288777469221L;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";

    public static Behavior<Message> create() {
        return Behaviors.setup(Worker::new);
    }

    private Worker(ActorContext<Message> context) {
        super(context);
        Reaper.watchWithDefaultReaper(this.getContext().getSelf());

        final int numWorkers = SystemConfigurationSingleton.get().getNumWorkers();
        if (numWorkers > 0) {
            // Spawn DataProvider BEFORE DependencyWorker to ensure it's available
            this.dataProvider = context.spawn(
                    DataProvider.create(),
                    DataProvider.DEFAULT_NAME);

            for (int i = 0; i < numWorkers; i++) {
                context.spawn(
                        DependencyWorker.create(this.dataProvider), // Pass DataProvider reference
                        DependencyWorker.DEFAULT_NAME + "-" + i,
                        DispatcherSelector.fromConfig("akka.worker-pinned-dispatcher"));
            }
            context.getLog().info("Worker System spawned 1 DataProvider, {} DependencyWorkers", numWorkers);
        }
    }

    /////////////////
    // Actor State //
    /////////////////

    private ActorRef<DataProvider.Message> dataProvider;
    private ActorRef<DependencyWorker.Message> dependencyWorker;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ShutdownMessage.class, this::handle)
                .build();
    }

    private Behavior<Message> handle(ShutdownMessage message) {
        return Behaviors.stopped();
    }
}
