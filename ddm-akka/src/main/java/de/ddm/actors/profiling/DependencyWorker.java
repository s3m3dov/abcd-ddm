package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.utils.LoggingInterceptor;
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
	@AllArgsConstructor
	public static class SetWorkerIdMessage implements Message {
		private static final long serialVersionUID = -3456789012345678901L;
		int workerId;
	}

	/**
	 * Task assignment from DependencyMiner
	 */
	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ValidateTaskMessage implements Message {
		private static final long serialVersionUID = -2765506199949499328L;
		int lhsAttributeId;
		int rhsAttributeId;
		ActorRef<DataProvider.Message> rhsDataProvider;
	}

	/**
	 * Column data received from DataProvider (adapted from
	 * DataProvider.ResponseColumnMessage)
	 */
	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ColumnDataMessage implements Message {
		private static final long serialVersionUID = -1234567890123456782L;
		int attributeId;
		Set<String> columnData;
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

	private ActorRef<DependencyMiner.Message> dependencyMiner;
	private int dependencyWorkerId = -1;

	// Current task state
	private ValidateTaskMessage currentTask = null;
	private Set<String> lhsData = null;
	private Set<String> rhsData = null;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(SetWorkerIdMessage.class, this::handle)
				.onMessage(ValidateTaskMessage.class, this::handle)
				.onMessage(ColumnDataMessage.class, this::handle)
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
					.tell(new DependencyMiner.DependencyWorkerRegistrationMessage(
							this.getContext().getSelf(),
							this.largeMessageProxy,
							this.localDataProvider)); // Include DataProvider reference
		}
		return this;
	}

	private Behavior<Message> handle(SetWorkerIdMessage message) {
		this.dependencyWorkerId = message.getWorkerId();
		this.getContext().getLog().info("DependencyWorker ID set to {}", this.dependencyWorkerId);
		return this;
	}

	private Behavior<Message> handle(ValidateTaskMessage message) {
		this.getContext().getLog().info("DependencyWorker-{} received validation task: {} ⊆ {}",
				this.dependencyWorkerId, message.getLhsAttributeId(), message.getRhsAttributeId());

		// Store current task
		this.currentTask = message;
		this.lhsData = null;
		this.rhsData = null;

		// Request both columns from their respective DataProviders
		// Include LargeMessageProxy reference for receiving large responses
		this.localDataProvider.tell(new DataProvider.RequestColumnMessage(
				message.getLhsAttributeId(), this.getContext().getSelf(), this.largeMessageProxy));
		message.getRhsDataProvider().tell(new DataProvider.RequestColumnMessage(
				message.getRhsAttributeId(), this.getContext().getSelf(), this.largeMessageProxy));

		return this;
	}

	private Behavior<Message> handle(ColumnDataMessage message) {
		// Store fetched column data
		if (message.getAttributeId() == this.currentTask.getLhsAttributeId()) {
			this.lhsData = message.getColumnData();
			this.getContext().getLog().debug("DependencyWorker-{} received LHS data for attribute {}",
					this.dependencyWorkerId, message.getAttributeId());
		} else if (message.getAttributeId() == this.currentTask.getRhsAttributeId()) {
			this.rhsData = message.getColumnData();
			this.getContext().getLog().debug("DependencyWorker-{} received RHS data for attribute {}",
					this.dependencyWorkerId, message.getAttributeId());
		}

		// When both ready, execute validation
		if (this.lhsData != null && this.rhsData != null) {
			this.executeValidation();
		}

		return this;
	}

	/////////////////
	// Actor Utils //
	/////////////////

	private void executeValidation() {
		// Check if LHS ⊆ RHS (all values in LHS exist in RHS)
		boolean isIND = this.rhsData.containsAll(this.lhsData);

		// Debug: Show sample data
		String lhsSample = Arrays.toString(this.lhsData.stream().limit(5).toArray(String[]::new));
		String rhsSample = Arrays.toString(this.rhsData.stream().limit(5).toArray(String[]::new));

		this.getContext().getLog().debug(
				"DependencyWorker-{} validation result for {} ⊆ {}: {} (LHS size: {}, RHS size: {}, LHS sample: {}, RHS sample: {})",
				this.dependencyWorkerId,
				this.currentTask.getLhsAttributeId(), this.lhsData.size(),
				this.currentTask.getRhsAttributeId(), this.rhsData.size(),
				isIND, lhsSample, rhsSample);

		// Send result to DependencyMiner
		this.dependencyMiner.tell(new DependencyMiner.ResultMessage(
				isIND ? new int[] { this.currentTask.getLhsAttributeId() } : new int[0],
				isIND ? new int[] { this.currentTask.getRhsAttributeId() } : new int[0],
				this.dependencyWorkerId));

		// Clear current task (ready for next assignment)
		this.currentTask = null;
		this.lhsData = null;
		this.rhsData = null;
	}

}
