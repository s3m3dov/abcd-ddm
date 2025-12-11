package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.DispatcherSelector;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.utils.LoggingInterceptor;
import it.unimi.dsi.fastutil.ints.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	public static class ValidateLocalCandidatesMessage implements Message {
		private static final long serialVersionUID = -2765506199949499328L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ShareForValidationMessage implements Message {
		private static final long serialVersionUID = -1228839983943869503L;
		int attribute;
		int[] untestedAttributes;
		ActorRef<DependencyWorker.Message> toWorker;
		int toWorkerId;
		ActorRef<LargeMessageProxy.Message> toWorkerLMP;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ValidateCandidatesMessage implements Message {
		private static final long serialVersionUID = -524894512354313788L;
		int attribute;
		List<String> column;
		int[] untestedAttributes;
		int workerId;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create(ActorRef<DataProvider.Message> dataProvider) {
		return LoggingInterceptor.wrap(Behaviors.setup(context -> new DependencyWorker(context, dataProvider)));
	}

	private DependencyWorker(ActorContext<Message> context, ActorRef<DataProvider.Message> dataProvider) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class,
				ReceptionistListingMessage::new);
		context.getSystem().receptionist()
				.tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(
				LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false),
				LargeMessageProxy.DEFAULT_NAME);

		// Store DataProvider reference passed from Worker
		this.localDataProvider = dataProvider;
		context.getLog().info("DependencyWorker initialized with DataProvider: {}", dataProvider.path());
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	private final ActorRef<DataProvider.Message> localDataProvider; // Local DataProvider on this node
	private List<ActorRef<DependencyWorkerHelper.Message>> dependencyWorkerHelpers;

	private ActorRef<DependencyMiner.Message> dependencyMiner;
	private int workerId = -1;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(ValidateLocalCandidatesMessage.class, this::handle)
				.onMessage(ShareForValidationMessage.class, this::handle)
				.onMessage(ValidateCandidatesMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {

		if (this.dependencyMiner != null)
			return this;

		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing()
				.getServiceInstances(DependencyMiner.dependencyMinerService);
		if (!dependencyMiners.isEmpty() && (this.dependencyMiner == null)) {
			this.dependencyMiner = dependencyMiners.iterator().next();
			this.dependencyMiner
					.tell(new DependencyMiner.RegistrationMessage(
							this.getContext().getSelf(),
							this.largeMessageProxy,
							this.localDataProvider)); // Include DataProvider reference
		}
		return this;
	}

	private Behavior<Message> handle(ValidateLocalCandidatesMessage message) {

		this.getContext().getLog().info("Validating all local INDs");

		this.initializeValidation();

		// Initialize the task messages for this validation round
		List<DependencyWorkerHelper.ValidateLocalTaskMessage> tasks = new ArrayList<>(
				this.dependencyWorkerHelpers.size());
		for (int i = 0; i < this.dependencyWorkerHelpers.size(); i++)
			tasks.add(new DependencyWorkerHelper.ValidateLocalTaskMessage(new IntArrayList(), new IntArrayList()));

		// Form all pairs of local attributes and assign them to the helper's tasks in a
		// round-robin fashion
        /*
		int nextHelper = 0;
		int[] attributes = message.getLocalAttributes(); // Attributes from DependencyMiner
		for (int i = 0; i < attributes.length - 1; i++) {
			for (int j = i + 1; j < attributes.length; j++) {
				tasks.get(nextHelper).getAttributes1().add(attributes[i]);
				tasks.get(nextHelper).getAttributes2().add(attributes[j]);
				nextHelper = (nextHelper == tasks.size() - 1) ? 0 : nextHelper + 1;
			}
		}
         */

		// Send all validation tasks to the corresponding helpers
		// Helpers will query DataProvider for actual column data
		for (int i = 0; i < this.dependencyWorkerHelpers.size(); i++)
			this.dependencyWorkerHelpers.get(i).tell(tasks.get(i));

		return this;
	}

	private Behavior<Message> handle(ShareForValidationMessage message) {

        /*
		this.initializeValidation();
		// Send the local column to the remote worker for IND candidate validation
		LargeMessageProxy.LargeMessage validateCandidatesMessage = new ValidateCandidatesMessage(
				message.getAttribute(), this.sortedColumns.get(message.getAttribute()), message.getUntestedAttributes(),
				message.getToWorkerId());
		this.largeMessageProxy
				.tell(new LargeMessageProxy.SendMessage(validateCandidatesMessage, message.getToWorkerLMP()));
		*/
		return this;
	}

	private Behavior<Message> handle(ValidateCandidatesMessage message) {

		this.getContext().getLog().info("Validating " + message.getUntestedAttributes().length + " INDs");

		this.initializeValidation();

		// Initialize the task messages for this validation round
		List<DependencyWorkerHelper.ValidateTaskMessage> tasks = new ArrayList<>(this.dependencyWorkerHelpers.size());
		for (int i = 0; i < this.dependencyWorkerHelpers.size(); i++)
			tasks.add(new DependencyWorkerHelper.ValidateTaskMessage(message.getAttribute(), message.getColumn(),
					new IntArrayList()));

		// Assign untested local columns to the helper's tasks in a round-robin fashion
		int nextHelper = 0;
		for (int localAttribute : message.getUntestedAttributes()) {
			tasks.get(nextHelper).getLocalAttributes().add(localAttribute);
			nextHelper = (nextHelper == tasks.size() - 1) ? 0 : nextHelper + 1;
		}

		// Send all validation tasks to the corresponding helpers
		for (int i = 0; i < this.dependencyWorkerHelpers.size(); i++)
			this.dependencyWorkerHelpers.get(i).tell(tasks.get(i));

		return this;
	}

	/////////////////
	// Actor Utils //
	/////////////////

	private void initializeValidation() {
		// Return if validation has already been initiated
		if (this.dependencyWorkerHelpers != null)
			return;

		// Spawn helper actors to perform the actual candidate validations
		// Helpers will request data from DataProvider as needed
		this.spawnHelperActors();
	}

	private void spawnHelperActors() {
		final int numWorkers = SystemConfigurationSingleton.get().getNumWorkers();

		// Spawn a cameo actor for the helpers to externalize the combination of the
		// results for miner
		ActorRef<DependencyWorkerCameo.Message> cameo = this.getContext().spawn(
				DependencyWorkerCameo.create(this.dependencyMiner, numWorkers, this.workerId),
				DependencyWorkerCameo.DEFAULT_NAME,
				DispatcherSelector.fromConfig("akka.worker-pool-dispatcher"));

		// Spawn the helper actors - they will request data from DataProvider as needed
        /*
		this.dependencyWorkerHelpers = new ArrayList<>(numWorkers);
		for (int id = 0; id < numWorkers; id++)
			this.dependencyWorkerHelpers.add(this.getContext().spawn(
					DependencyWorkerHelper.create(this.localDataProvider, cameo),
					DependencyWorkerHelper.DEFAULT_NAME + "_" + id,
					DispatcherSelector.fromConfig("akka.worker-pool-dispatcher")));
        */
	}

}
