package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.utils.LoggingInterceptor;
import de.ddm.structures.InclusionDependency;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.booleans.BooleanList;
import it.unimi.dsi.fastutil.ints.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int readerId;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int readerId;
		String[][] batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class StatusMessage implements Message {
		private static final long serialVersionUID = -5302922080219823523L;
		int dataProviderId;
		boolean active;
		boolean overloaded;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DataProviderRegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914108L;
		ActorRef<DataProvider.Message> dataProvider;
		ActorRef<LargeMessageProxy.Message> dataProviderLMP;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DependencyWorkerRegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914109L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> dependencyWorkerLMP;
		ActorRef<DataProvider.Message> localDataProvider; // Co-located DataProvider
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ResultMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		int[] lhss;
		int[] rhss;
		int workerId;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey
			.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return LoggingInterceptor.wrap(Behaviors.setup(DependencyMiner::new));
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		// this.discoverNaryDependencies =
		// SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(this.inputFiles.length);
		this.inputReaderFinished = new BooleanArrayList();
		for (int id = 0; id < this.inputFiles.length; id++) {
			this.inputReaders.add(
					context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
			this.inputReaderFinished.add(false);
		}

		this.pendingBatchMessages = new LinkedList<>();
		this.pendingWorkerRegistrations = new LinkedList<>();

		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(
				LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast(), false),
				LargeMessageProxy.DEFAULT_NAME);

		this.dataProviders = new ArrayList<>();
		this.dataProviderLMPs = new ArrayList<>();
		this.dataProvidersActive = new BooleanArrayList();
		this.dataProvidersOverloaded = new BooleanArrayList();

		this.dependencyWorkers = new ArrayList<>();
		this.dependencyWorkerLMPs = new ArrayList<>();
		this.dependencyWorkersActive = new BooleanArrayList();

		this.dataProvider2workers = new HashMap<>();
		this.worker2dataProvider = new HashMap<>();

		// Initialize transitivity pruning graph
		this.indGraph = new HashMap<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private static final int batchSize = 10000;

	private long startTime;

	// private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final BooleanList inputReaderFinished;

	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	// DataProvider tracking (one per worker node)
	private final List<ActorRef<DataProvider.Message>> dataProviders;
	private final List<ActorRef<LargeMessageProxy.Message>> dataProviderLMPs;
	private final BooleanList dataProvidersActive;
	private final BooleanList dataProvidersOverloaded;

	// DependencyWorker tracking (multiple per worker node)
	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;
	private final List<ActorRef<LargeMessageProxy.Message>> dependencyWorkerLMPs;
	private final BooleanList dependencyWorkersActive;

	// Co-location tracking: maps dataProviderId to list of workerIds on same node
	private final Map<Integer, List<Integer>> dataProvider2workers;
	private final Map<Integer, Integer> worker2dataProvider; // Reverse mapping: workerId -> dataProviderId

	// Attribute mappings
	private HashMap<File, int[]> file2attribute;
	private File[] attribute2file;
	private String[] attribute2name;
	private int[] attribute2dataProvider; // Maps attribute to DataProvider ID

	// Pending messages
	private final List<BatchMessage> pendingBatchMessages;
	private final List<DependencyWorkerRegistrationMessage> pendingWorkerRegistrations;

	// Validation phase state
	private Map<Integer, Queue<int[]>> workerTaskQueues; // Per-worker task queues for locality
	private int totalTasks; // Total tasks generated (never changes after initialization)
	private int completedTasks;
	private int prunedTasks; // Tasks skipped due to transitivity pruning
	private int totalINDsFound;

	// Transitivity pruning: Graph-based IND storage for efficient transitivity
	// checks
	// Maps lhs -> set of rhs where lhs ⊆ rhs
	private Map<Integer, Set<Integer>> indGraph;

	// Rebalancing state
	private IntSet rebalancingDataProviderIds = new IntOpenHashSet(); // IDs of DataProviders involved in rebalancing
	private IntList awaitingDataProviderIds = new IntArrayList(); // IDs of DataProviders waiting for attributes

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(DataProviderRegistrationMessage.class, this::handle)
				.onMessage(DependencyWorkerRegistrationMessage.class, this::handle)
				.onMessage(StatusMessage.class, this::handle)
				.onMessage(ResultMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		this.startTime = System.currentTimeMillis();

		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.getContext().getLog().info("HeaderMessage Received: {} active DataProviders",
				this.dataProvidersActive.stream().filter(b -> b).count());

		// Store the received header lines
		this.headerLines[message.getReaderId()] = message.getHeader();

		// Test if header lines are still missing and in case a header is still missing
		// simply wait
		if (this.isHeaderPhase())
			return this;

		// Create attribute IDs and mappings to and from attribute descriptors, so we
		// can work with IDs during IND discovery
		int numAttributes = 0;
		this.file2attribute = new HashMap<>();
		for (int i = 0; i < this.headerLines.length; i++) {
			file2attribute.put(this.inputFiles[i], new int[this.headerLines[i].length]);
			numAttributes += this.headerLines[i].length;
		}

		this.attribute2file = new File[numAttributes];
		this.attribute2name = new String[numAttributes];
		int attribute = 0;
		for (int i = 0; i < this.inputFiles.length; i++) {
			File file = this.inputFiles[i];
			for (int j = 0; j < this.headerLines[i].length; j++) {
				String name = this.headerLines[i][j];
				this.attribute2file[attribute] = file;
				this.attribute2name[attribute] = name;
				this.file2attribute.get(file)[j] = attribute;
				attribute++;
			}
		}

		// Wait if no workers have registered yet
		if (this.dataProviders.isEmpty())
			return this;

		// Map data providers to attributes and start reading
		this.mapAttributesToDataProviders();
		this.startReading();

		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		this.getContext().getLog().info("BatchMessage Received: {} active data providers",
				this.dataProvidersActive.stream().filter(b -> b).count());

		// Block batches during rebalancing
		if (this.isRebalancing()) {
			this.getContext().getLog().info("Delaying batch processing because rebalancing is in progress");
			this.pendingBatchMessages.add(message);
			return this;
		}

		// Close the reading process for this file if the last line was read
		if (message.getBatch().length == 0) {
			this.inputReaderFinished.set(message.getReaderId(), true);

			if (this.isValidationPhase()) {
				// Log the result of the data reading process
				this.getContext().getLog().info("Reading data completed in {}ms",
						System.currentTimeMillis() - this.startTime);
				// Output current DependencyMiner information for debugging
				this.outputDebugInformation();
				// Send debug info to each DataProvider
				this.requestDebugInfoFromDataProviders();

				// Attempt to initiate the validation phase
				this.initiateValidationPhase();
			}
			return this;
		}

		// Wait for more workers to join and steal if any number of workers is
		// overloaded
		if (this.dataProvidersOverloaded.stream().anyMatch(b -> b)) {
			this.getContext().getLog()
					.info("Delaying batch processing because one or more DataProviders are overloaded");
			this.pendingBatchMessages.add(message);
			return this;
		}

		// Wait for DataProviders to become inactive if one (or more) target
		// DataProviders are
		// currently already active
		File file = this.inputFiles[message.getReaderId()];
		String[][] columns = message.getBatch();
		for (int i = 0; i < columns.length; i++) {
			int attribute = this.file2attribute.get(file)[i];
			int dataProviderId = this.attribute2dataProvider[attribute];
			if (this.dataProvidersActive.getBoolean(dataProviderId)) {
				this.getContext().getLog().info(
						"Delaying batch processing because target DataProvider-{} is still active", dataProviderId);
				this.pendingBatchMessages.add(message);
				return this;
			}
		}

		// Send the columns and set the corresponding DataProviders to active
		for (int i = 0; i < columns.length; i++) {
			int attributeId = this.file2attribute.get(file)[i];
			int dataProviderId = this.attribute2dataProvider[attributeId];
			int numColumnsForThisDataProvider = 0;
			for (int j = 0; j < columns.length; j++)
				if (this.attribute2dataProvider[this.file2attribute.get(file)[j]] == dataProviderId)
					numColumnsForThisDataProvider++;

			// Send DataMessage via LargeMessageProxy to handle large payloads
			LargeMessageProxy.LargeMessage dataMessage = new DataProvider.DataMessage(
					attributeId, columns[i], numColumnsForThisDataProvider);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(
					dataMessage, this.dataProviderLMPs.get(dataProviderId)));
			this.dataProvidersActive.set(dataProviderId, true);
		}

		// Read the next batch from the input reader whose batch we just forwarded
		this.inputReaders.get(message.getReaderId())
				.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), batchSize));

		return this;
	}

	private Behavior<Message> handle(StatusMessage message) {
		this.getContext().getLog().info("StatusMessage Received: {} Active DataProviders",
				this.dataProvidersActive.stream().filter(b -> b).count());

		this.dataProvidersActive.set(message.getDataProviderId(), message.isActive());
		this.dataProvidersOverloaded.set(message.getDataProviderId(), message.isOverloaded());

		// Track rebalancing completion
		if (this.isRebalancing()) {
			this.rebalancingDataProviderIds.remove(message.getDataProviderId());

			// Check if rebalancing is complete (all involved DataProviders reported)
			if (this.rebalancingDataProviderIds.isEmpty()) {
				this.getContext().getLog().info("Rebalancing complete, resuming batch processing");

				// Process pending batches
				for (BatchMessage batchMessage : this.pendingBatchMessages)
					this.getContext().getSelf().tell(batchMessage);
				this.pendingBatchMessages.clear();
				return this;
			}
		}

		// Check if we are already in validation phase; in that case, StatusMessages are
		// irrelevant, but we might still
		// need to initiate the validation phase, if this was the last active
		// DataProvider
		if (this.isValidationPhase()) {
			this.initiateValidationPhase();
			return this;
		}

		// Trigger rebalancing immediately if overloaded and awaiting DataProviders
		// exist
		if (this.isBatchPhase()
				&& !this.isRebalancing()
				&& this.dataProvidersOverloaded.stream().anyMatch(b -> b) // Any overloaded
				&& !this.awaitingDataProviderIds.isEmpty()) {

			this.getContext().getLog().info("Overload detected with awaiting DataProviders, triggering rebalancing");
			this.triggerRebalancing();
			return this;
		}

		// Trigger rebalancing at batch boundary (all idle)
		if (this.isBatchPhase()
				&& !this.isRebalancing()
				&& this.dataProvidersActive.stream().noneMatch(b -> b) // All idle
				&& !this.awaitingDataProviderIds.isEmpty()) {

			this.getContext().getLog().info("Batch boundary reached, triggering rebalancing");
			this.triggerRebalancing();
			return this;
		}

		// Resume batch processing if not rebalancing
		if (!this.isRebalancing() && !this.pendingBatchMessages.isEmpty()) {
			for (BatchMessage batchMessage : this.pendingBatchMessages)
				this.getContext().getSelf().tell(batchMessage);
			this.pendingBatchMessages.clear();
		}

		return this;
	}

	private Behavior<Message> handle(DataProviderRegistrationMessage message) {
		this.getContext().getLog().info("DataProviderRegistrationMessage received");

		ActorRef<DataProvider.Message> dataProvider = message.getDataProvider();
		ActorRef<LargeMessageProxy.Message> dataProviderLMP = message.getDataProviderLMP();

		// Ignore if already registered
		if (this.dataProviders.contains(dataProvider))
			return this;

		// Register immediately
		this.registerNewDataProvider(dataProvider, dataProviderLMP);
		int dataProviderId = this.dataProviders.size() - 1;

		// Process any pending worker registrations for this DataProvider
		this.processPendingWorkerRegistrations(dataProvider);

		// First DataProvider: initialize and start reading
		if (this.isBatchPhase() && dataProviderId == 0) {
			this.mapAttributesToDataProviders();
			this.startReading();
			return this;
		}

		// Validation phase: too late to join
		if (this.isValidationPhase())
			return this;

		// Loading phase: add to awaiting list for rebalancing
		this.awaitingDataProviderIds.add(dataProviderId);
		this.getContext().getLog().info("DataProvider-{} awaiting rebalancing", dataProviderId);

		// Trigger immediate rebalancing if overloaded
		if (!this.isRebalancing() && this.dataProvidersOverloaded.stream().anyMatch(b -> b)) {
			this.getContext().getLog().info("Overload detected, triggering immediate rebalancing");
			this.triggerRebalancing();
		}

		return this;
	}

	private Behavior<Message> handle(DependencyWorkerRegistrationMessage message) {
		this.getContext().getLog().info("WorkerRegistrationMessage received");

		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		ActorRef<LargeMessageProxy.Message> dependencyWorkerLMP = message.getDependencyWorkerLMP();
		ActorRef<DataProvider.Message> localDataProvider = message.getLocalDataProvider();

		// Ignore if already registered
		if (this.dependencyWorkers.contains(dependencyWorker))
			return this;

		// DataProvider must exist (it registers first)
		if (!this.dataProviders.contains(localDataProvider)) {
			this.getContext().getLog().warn("Worker registration before DataProvider - queuing for later processing");
			this.pendingWorkerRegistrations.add(message);
			return this;
		}

		// Register the worker with its co-located DataProvider
		this.registerNewWorker(dependencyWorker, dependencyWorkerLMP, localDataProvider);
		return this;
	}

	private Behavior<Message> handle(ResultMessage message) {
		this.completedTasks++;
		this.getContext().getLog().info("ResultMessage! {} INDs from DependencyWorker-{} ({} of {} tasks completed)",
				message.getLhss().length, message.getWorkerId(), this.completedTasks, this.totalTasks);

		// Store discovered INDs in graph for transitivity pruning
		for (int i = 0; i < message.getLhss().length; i++) {
			int lhs = message.getLhss()[i];
			int rhs = message.getRhss()[i];

			// Add edge lhs -> rhs to the graph (lhs ⊆ rhs)
			this.indGraph.computeIfAbsent(lhs, k -> new HashSet<>()).add(rhs);
		}

		// Create Inclusion Dependency objects from the results
		List<InclusionDependency> inds = new ArrayList<>(message.getLhss().length);
		for (int i = 0; i < message.getLhss().length; i++) {
			String[] dependentAttributes = { this.attribute2name[message.getLhss()[i]] };
			String[] referencedAttributes = { this.attribute2name[message.getRhss()[i]] };
			inds.add(new InclusionDependency(this.attribute2file[message.getLhss()[i]], dependentAttributes,
					this.attribute2file[message.getRhss()[i]], referencedAttributes));
		}

		// Send Inclusion Dependency objects to the result collector
		if (!inds.isEmpty()) {
			this.totalINDsFound += inds.size();
			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
		}

		// Assign next task to this worker (work-pulling)
		this.assignNextValidationTask(message.getWorkerId());

		return this;
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}

	/////////////////
	// Actor Utils //
	/////////////////

	// * PHASE CHECKS * //
	private boolean isHeaderPhase() {
		// The header phase lasts as long as header lines are still missing
		return Arrays.stream(this.headerLines).anyMatch(Objects::isNull);
	}

	private boolean isBatchPhase() {
		// The batch phase lasts as long as input readers are still unfinished and
		// batches are pending
		return !this.isHeaderPhase() &&
				(this.inputReaderFinished.stream().anyMatch(b -> !b) ||
						!this.pendingBatchMessages.isEmpty());
	}

	private boolean isRebalancing() {
		// Part of BatchPhase
		// Rebalancing is ongoing if there are still DataProviders involved
		return !this.rebalancingDataProviderIds.isEmpty();
	}

	private boolean isValidationPhase() {
		// The validation phase lasts until the algorithm ends
		return !this.isBatchPhase();
	}

	// * REGISTRATION * //

	private void registerNewDataProvider(ActorRef<DataProvider.Message> dataProvider,
			ActorRef<LargeMessageProxy.Message> dataProviderLMP) {
		this.dataProviders.add(dataProvider);
		this.dataProviderLMPs.add(dataProviderLMP);
		this.dataProvidersActive.add(false);
		this.dataProvidersOverloaded.add(false);

		// DataProvider ID is its index in the list
		int dataProviderId = this.dataProviders.size() - 1;

		// Send ID immediately
		dataProvider.tell(new DataProvider.SetDataProviderIdMessage(dataProviderId));

		this.getContext().getLog().info("Registered DataProvider-{}", dataProviderId);
	}

	private void registerNewWorker(ActorRef<DependencyWorker.Message> dependencyWorker,
			ActorRef<LargeMessageProxy.Message> dependencyWorkerLMP, ActorRef<DataProvider.Message> localDataProvider) {
		this.dependencyWorkers.add(dependencyWorker);
		this.dependencyWorkerLMPs.add(dependencyWorkerLMP);
		this.getContext().watch(dependencyWorker);
		this.dependencyWorkersActive.add(false);

		// Track co-location: add workerId to DataProvider's worker list
		int workerId = this.dependencyWorkers.size() - 1;
		int dataProviderId = this.dataProviders.indexOf(localDataProvider);
		this.dataProvider2workers.computeIfAbsent(dataProviderId, k -> new ArrayList<>()).add(workerId);
		this.worker2dataProvider.put(workerId, dataProviderId); // Reverse mapping

		this.getContext().getLog().info("Registered DependencyWorker-{} co-located with DataProvider-{}",
				workerId, dataProviderId);

		// Send the assigned worker ID back to the worker
		dependencyWorker.tell(new DependencyWorker.SetWorkerIdMessage(workerId));
	}

	/**
	 * Process pending worker registrations for a newly registered DataProvider
	 */
	private void processPendingWorkerRegistrations(ActorRef<DataProvider.Message> dataProvider) {
		if (this.pendingWorkerRegistrations.isEmpty())
			return;

		// Find all workers waiting for this DataProvider
		List<DependencyWorkerRegistrationMessage> toProcess = new ArrayList<>();
		for (DependencyWorkerRegistrationMessage msg : this.pendingWorkerRegistrations) {
			if (msg.getLocalDataProvider().equals(dataProvider)) {
				toProcess.add(msg);
			}
		}

		// Remove from pending and send messages back to self for processing
		for (DependencyWorkerRegistrationMessage msg : toProcess) {
			this.pendingWorkerRegistrations.remove(msg);
			this.getContext().getLog().info("Re-sending pending worker registration for processing");
			this.getContext().getSelf().tell(msg);
		}
	}

	// * MAPPING & READING * //
	private void mapAttributesToDataProviders() {
		int numDataProviders = this.dataProviders.size();
		int numAttributes = this.attribute2name.length;

		// Assign attributes to DataProviders with a round-robin strategy so that long
		// and
		// short columns are assigned approximately evenly to DataProviders
		this.attribute2dataProvider = new int[numAttributes];
		for (int attribute = 0; attribute < numAttributes; attribute++)
			this.attribute2dataProvider[attribute] = attribute % numDataProviders;

		this.getContext().getLog().info("Mapped {} attributes to {} DataProviders",
				numAttributes, numDataProviders);
	}

	private void startReading() {
		// Initiate reading of the first batches
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), batchSize));

		// Inform the DataProviders about their initial responsibilities
		for (int dataProviderId = 0; dataProviderId < this.dataProviders.size(); dataProviderId++) {
			IntArrayList attributes = new IntArrayList();
			for (int attribute = 0; attribute < this.attribute2dataProvider.length; attribute++)
				if (this.attribute2dataProvider[attribute] == dataProviderId)
					attributes.add(attribute);

			this.dataProviders.get(dataProviderId)
					.tell(new DataProvider.InitialAttributesMessage(attributes.toArray(new int[0])));
			this.dataProvidersActive.set(dataProviderId, true);
		}
	}

	// * REBALANCING * //
	/**
	 * Trigger rebalancing by redistributing attributes to new DataProviders.
	 * Delegates to appropriate strategy based on whether overloaded DataProviders
	 * exist.
	 */
	private void triggerRebalancing() {
		int numDataProviders = this.dataProviders.size();
		int numAttributes = this.attribute2name.length;
		int targetAttributesPerDP = numAttributes / numDataProviders;

		this.getContext().getLog().info("Rebalancing {} attributes across {} DataProviders (target: {} each)",
				numAttributes, numDataProviders, targetAttributesPerDP);

		// Calculate current distribution
		int[] currentCounts = new int[numDataProviders];
		for (int attribute = 0; attribute < numAttributes; attribute++) {
			currentCounts[this.attribute2dataProvider[attribute]]++;
		}

		// Log BEFORE state
		logAttributeDistribution("BEFORE rebalancing", currentCounts);

		// Check if any DataProviders are overloaded
		boolean hasOverloaded = this.dataProvidersOverloaded.stream().anyMatch(b -> b);

		if (hasOverloaded) {
			// Use overload-priority strategy
			this.getContext().getLog().info("Using overload-priority rebalancing strategy");
			rebalanceWithOverloadPriority(numDataProviders, targetAttributesPerDP, currentCounts);
		} else {
			// Use batch-wise (even distribution) strategy
			this.getContext().getLog().info("Using batch-wise rebalancing strategy");
			rebalanceBatchWise(numDataProviders, targetAttributesPerDP, currentCounts);
		}

		// Clear awaiting list
		this.awaitingDataProviderIds.clear();

		// Log AFTER state
		logAttributeDistribution("AFTER rebalancing", currentCounts);

		this.getContext().getLog().info("Rebalancing in progress, batches delayed");
	}

	/**
	 * Batch-wise rebalancing: Evenly distribute attributes across all
	 * DataProviders.
	 * This is the original strategy - steal from any DataProvider with excess
	 * columns.
	 */
	private void rebalanceBatchWise(int numDataProviders, int targetAttributesPerDP, int[] currentCounts) {
		for (int awaitingDataProvider : this.awaitingDataProviderIds) {
			Int2IntMap attribute2sourceDP = new Int2IntOpenHashMap();

			// Steal from all DataProviders with excess columns
			stealColumnsFromSources(
					awaitingDataProvider,
					numDataProviders,
					targetAttributesPerDP,
					currentCounts,
					attribute2sourceDP,
					sourceDP -> true // Accept any DataProvider as source
			);

			// Complete the rebalancing for this awaiting DataProvider
			completeRebalancing(awaitingDataProvider, attribute2sourceDP);
		}
	}

	/**
	 * Overload-priority rebalancing: Prioritize stealing from overloaded
	 * DataProviders.
	 * Uses a two-phase approach:
	 * Phase 1: Steal from overloaded DataProviders first
	 * Phase 2: Steal from non-overloaded DataProviders if needed
	 */
	private void rebalanceWithOverloadPriority(int numDataProviders, int targetAttributesPerDP, int[] currentCounts) {
		for (int awaitingDataProvider : this.awaitingDataProviderIds) {
			Int2IntMap attribute2sourceDP = new Int2IntOpenHashMap();

			// PHASE 1: Steal from overloaded DataProviders
			int stolenFromOverloaded = stealColumnsFromSources(
					awaitingDataProvider,
					numDataProviders,
					targetAttributesPerDP,
					currentCounts,
					attribute2sourceDP,
					sourceDP -> this.dataProvidersOverloaded.getBoolean(sourceDP));

			if (stolenFromOverloaded > 0) {
				this.getContext().getLog().info(
						"Phase 1: Stole {} columns from overloaded DataProviders for DataProvider-{}",
						stolenFromOverloaded, awaitingDataProvider);
			}

			// PHASE 2: Steal from non-overloaded DataProviders if still needed
			int stolenFromNonOverloaded = stealColumnsFromSources(
					awaitingDataProvider,
					numDataProviders,
					targetAttributesPerDP,
					currentCounts,
					attribute2sourceDP,
					sourceDP -> !this.dataProvidersOverloaded.getBoolean(sourceDP));

			if (stolenFromNonOverloaded > 0) {
				this.getContext().getLog().info(
						"Phase 2: Stole {} columns from non-overloaded DataProviders for DataProvider-{}",
						stolenFromNonOverloaded, awaitingDataProvider);
			}

			// Complete the rebalancing for this awaiting DataProvider
			completeRebalancing(awaitingDataProvider, attribute2sourceDP);
		}
	}

	/**
	 * Helper method to steal columns from source DataProviders.
	 * 
	 * @param awaitingDataProvider  The DataProvider receiving columns
	 * @param numDataProviders      Total number of DataProviders
	 * @param targetAttributesPerDP Target number of attributes per DataProvider
	 * @param currentCounts         Current attribute counts for each DataProvider
	 * @param attribute2sourceDP    Map tracking which attributes came from which
	 *                              source
	 * @param sourceFilter          Predicate to filter which DataProviders to steal
	 *                              from
	 * @return Number of columns stolen
	 */
	private int stealColumnsFromSources(
			int awaitingDataProvider,
			int numDataProviders,
			int targetAttributesPerDP,
			int[] currentCounts,
			Int2IntMap attribute2sourceDP,
			java.util.function.IntPredicate sourceFilter) {

		int needed = targetAttributesPerDP - currentCounts[awaitingDataProvider];
		int stolen = 0;

		for (int sourceDataProvider = 0; sourceDataProvider < numDataProviders; sourceDataProvider++) {
			if (sourceDataProvider == awaitingDataProvider)
				continue;

			// Apply filter to determine if we should steal from this source
			if (!sourceFilter.test(sourceDataProvider))
				continue;

			while (currentCounts[sourceDataProvider] > targetAttributesPerDP && needed > 0) {
				// Find a random attribute owned by sourceDP
				int attribute = findRandomAttribute(sourceDataProvider);

				// Track the source BEFORE updating mapping
				attribute2sourceDP.put(attribute, sourceDataProvider);

				// Update mapping immediately
				this.attribute2dataProvider[attribute] = awaitingDataProvider;
				currentCounts[sourceDataProvider]--;
				currentCounts[awaitingDataProvider]++;
				needed--;
				stolen++;
			}
		}

		return stolen;
	}

	/**
	 * Complete the rebalancing process for an awaiting DataProvider.
	 * Sends move messages and tracks rebalancing state.
	 */
	private void completeRebalancing(int awaitingDataProvider, Int2IntMap attribute2sourceDP) {
		// Send MoveDataMessages to source DataProviders
		sendMoveDataMessages(awaitingDataProvider, attribute2sourceDP);

		// Track source DataProviders involved in rebalancing
		for (int sourceDP : attribute2sourceDP.values()) {
			this.rebalancingDataProviderIds.add(sourceDP);
		}
	}

	/**
	 * Find a random attribute owned by the specified DataProvider.
	 */
	private int findRandomAttribute(int dataProviderId) {
		IntList candidates = new IntArrayList();
		for (int i = 0; i < this.attribute2dataProvider.length; i++) {
			if (this.attribute2dataProvider[i] == dataProviderId) {
				candidates.add(i);
			}
		}
		if (candidates.isEmpty()) {
			throw new IllegalStateException("DataProvider-" + dataProviderId + " has no attributes");
		}
		return candidates.getInt(new Random().nextInt(candidates.size()));
	}

	/**
	 * Send MoveDataMessages to source DataProviders to move attributes to target.
	 */
	private void sendMoveDataMessages(int toDataProviderId, Int2IntMap attribute2sourceDP) {
		// Group attributes by source DataProvider
		Int2ObjectMap<IntList> sourceDP2attributes = new Int2ObjectArrayMap<>();
		for (Int2IntMap.Entry entry : attribute2sourceDP.int2IntEntrySet()) {
			int attribute = entry.getIntKey();
			int sourceDP = entry.getIntValue();
			sourceDP2attributes.computeIfAbsent(sourceDP, k -> new IntArrayList()).add(attribute);
		}

		// Send MoveDataMessage to each source
		ActorRef<DataProvider.Message> toDataProvider = this.dataProviders.get(toDataProviderId);
		ActorRef<LargeMessageProxy.Message> toDataProviderLMP = this.dataProviderLMPs.get(toDataProviderId);

		for (Int2ObjectMap.Entry<IntList> entry : sourceDP2attributes.int2ObjectEntrySet()) {
			int fromDataProviderId = entry.getIntKey();
			int[] attributes = entry.getValue().toIntArray();

			DataProvider.MoveDataMessage msg = new DataProvider.MoveDataMessage(attributes, toDataProviderId,
					toDataProvider, toDataProviderLMP, sourceDP2attributes.size());

			this.dataProviders.get(fromDataProviderId).tell(msg);
			this.dataProvidersActive.set(fromDataProviderId, true);
		}

		this.dataProvidersActive.set(toDataProviderId, true);
	}

	/**
	 * Log detailed attribute distribution showing counts and attribute IDs for each
	 * DataProvider.
	 */
	private void logAttributeDistribution(String label, int[] attributeCounts) {
		StringBuilder summary = new StringBuilder(label + " - Counts: ");
		for (int dpId = 0; dpId < attributeCounts.length; dpId++) {
			summary.append("DP-").append(dpId).append(": ").append(attributeCounts[dpId]);
			if (dpId < attributeCounts.length - 1) {
				summary.append(", ");
			}
		}
		this.getContext().getLog().info(summary.toString());

		// Log detailed attribute IDs for each DataProvider
		for (int dpId = 0; dpId < this.dataProviders.size(); dpId++) {
			IntList attributeIds = new IntArrayList();
			for (int attribute = 0; attribute < this.attribute2dataProvider.length; attribute++) {
				if (this.attribute2dataProvider[attribute] == dpId) {
					attributeIds.add(attribute);
				}
			}
			this.getContext().getLog().info("{} - DataProvider-{}: {} attributes -> {}",
					label, dpId, attributeIds.size(), attributeIds);
		}
	}

	// * VALIDATION PHASE * //
	private void initiateValidationPhase() {
		int numAttributes = this.attribute2name.length;
		int numWorkers = this.dependencyWorkers.size();

		// Check that all DataProviders are inactive; otherwise, wait
		if (this.dataProvidersActive.stream().anyMatch(b -> b))
			return;

		this.getContext().getLog().info("Initiating validation phase with {} attributes and {} workers",
				numAttributes, numWorkers);

		// Tell all DataProviders to transition to validation phase (sort columns and
		// free memory)
		this.getContext().getLog().info("Instructing all DataProviders to transition to validation phase");
		for (ActorRef<DataProvider.Message> dataProvider : this.dataProviders) {
			dataProvider.tell(new DataProvider.TransitionToValidationPhaseMessage());
		}

		// Generate per-worker task queues based on locality
		// Each worker gets tasks where LHS attribute is local to its DataProvider
		this.workerTaskQueues = new HashMap<>();
		for (int workerId = 0; workerId < numWorkers; workerId++) {
			this.workerTaskQueues.put(workerId, new LinkedList<>());
		}

		// Distribute tasks to workers based on LHS locality
		int totalTasksGenerated = 0;
		for (int lhsAttr = 0; lhsAttr < numAttributes; lhsAttr++) {
			// Find which DataProvider holds the LHS attribute
			int lhsDataProviderId = this.attribute2dataProvider[lhsAttr];

			// Get workers co-located with this DataProvider
			List<Integer> colocatedWorkers = this.dataProvider2workers.get(lhsDataProviderId);

			if (colocatedWorkers == null || colocatedWorkers.isEmpty()) {
				this.getContext().getLog().warn(
						"No workers co-located with DataProvider-{} for attribute {}, skipping tasks",
						lhsDataProviderId, lhsAttr);
				continue;
			}

			// Assign ALL tasks for this LHS attribute to a single worker (round-robin by
			// LHS)
			// This ensures the worker loads the LHS once and validates against all RHS
			// attributes
			int targetWorkerId = colocatedWorkers.get(lhsAttr % colocatedWorkers.size());

			for (int rhsAttr = 0; rhsAttr < numAttributes; rhsAttr++) {
				// Only generate task if lhsAttr < rhsAttr to avoid duplicates
				// DependencyWorker will check both directions (lhs ⊆ rhs AND rhs ⊆ lhs)
				if (lhsAttr < rhsAttr) {
					this.workerTaskQueues.get(targetWorkerId).add(new int[] { lhsAttr, rhsAttr });
					totalTasksGenerated++;
				}
			}
		}

		this.totalTasks = totalTasksGenerated;
		this.completedTasks = 0;
		this.prunedTasks = 0; // Reset pruned task counter
		this.totalINDsFound = 0;

		this.getContext().getLog().info("Generated {} validation tasks distributed across {} DependencyWorkers",
				this.totalTasks, numWorkers);

		// Log task distribution
		for (int workerId = 0; workerId < numWorkers; workerId++) {
			int taskCount = this.workerTaskQueues.get(workerId).size();
			this.getContext().getLog().info("DependencyWorker-{} has assigned {} tasks", workerId, taskCount);
		}

		// Assign initial task to each worker
		for (int workerId = 0; workerId < numWorkers; workerId++) {
			this.assignNextValidationTask(workerId);
		}
	}

	/**
	 * Assign next validation task to a worker (locality-aware work-pulling pattern)
	 * Each worker only gets tasks where the LHS attribute is local to its
	 * co-located DataProvider
	 */
	private void assignNextValidationTask(int dependencyWorkerId) {
		Queue<int[]> workerQueue = this.workerTaskQueues.get(dependencyWorkerId);

		if (workerQueue == null || workerQueue.isEmpty()) {
			// No more work for this worker
			this.dependencyWorkersActive.set(dependencyWorkerId, false);

			// Check if all workers are done
			if (this.dependencyWorkersActive.stream().noneMatch(b -> b)) {
				// Double-check: only finalize if we've actually started processing tasks
				// (i.e., completedTasks > 0 or totalTasks == 0)
				if (this.completedTasks > 0 || this.totalTasks == 0) {
					this.resultCollector.tell(new ResultCollector.FinalizeMessage());
					long discoveryTime = System.currentTimeMillis() - this.startTime;
					// Log mining completion
					this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
					// Log transitivity pruning statistics
					if (this.totalTasks > 0) {
						double pruningPercentage = (this.prunedTasks * 100.0) / this.totalTasks;
						this.getContext().getLog().info(
								"Transitivity Pruning: {} tasks pruned out of {} ({}%)",
								this.prunedTasks, this.totalTasks,
								String.format("%.2f", pruningPercentage));
					}
					// Log total INDs found
					this.getContext().getLog().info("Total Inclusion Dependencies found: {}", this.totalINDsFound);
				} else {
					this.getContext().getLog().debug(
							"All workers inactive but no tasks completed yet - waiting for task processing to begin");
				}
			}
			return;
		}

		// Get next task from this worker's queue
		int[] task = workerQueue.poll();
		int lhsAttr = task[0];
		int rhsAttr = task[1];

		// Check if this task can be pruned via bidirectional transitivity
		if (canPruneByTransitivity(lhsAttr, rhsAttr)) {
			this.prunedTasks++;
			this.completedTasks++; // Count as completed to maintain progress tracking

			// Since we only prune when BOTH directions can be inferred,
			// we need to add both directions to the graph and report both INDs

			// Add both directions to graph for future transitivity checks
			this.indGraph.computeIfAbsent(lhsAttr, k -> new HashSet<>()).add(rhsAttr);
			this.indGraph.computeIfAbsent(rhsAttr, k -> new HashSet<>()).add(lhsAttr);

			// Create and report both INDs (we know both are valid through transitivity)
			List<InclusionDependency> inds = new ArrayList<>();

			// IND 1: lhs ⊆ rhs
			inds.add(new InclusionDependency(
					this.attribute2file[lhsAttr], new String[] { this.attribute2name[lhsAttr] },
					this.attribute2file[rhsAttr], new String[] { this.attribute2name[rhsAttr] }));

			// IND 2: rhs ⊆ lhs
			inds.add(new InclusionDependency(
					this.attribute2file[rhsAttr], new String[] { this.attribute2name[rhsAttr] },
					this.attribute2file[lhsAttr], new String[] { this.attribute2name[lhsAttr] }));

			this.totalINDsFound += 2;
			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));

			this.getContext().getLog().debug(
					"Pruned task ({} ⊆ {}? AND {} ⊆ {}?) via bidirectional transitivity - both INDs reported",
					lhsAttr, rhsAttr, rhsAttr, lhsAttr);

			// Recursively assign next task
			this.assignNextValidationTask(dependencyWorkerId);
			return;
		}

		// Verify locality (LHS should be local to this worker's DataProvider)
		int lhsDataProviderId = this.attribute2dataProvider[lhsAttr];
		int workerDataProviderId = this.worker2dataProvider.get(dependencyWorkerId);

		if (lhsDataProviderId != workerDataProviderId) {
			this.getContext().getLog().error(
					"LOCALITY VIOLATION: Worker-{} (DP-{}) assigned task with LHS attr {} from DP-{}",
					dependencyWorkerId, workerDataProviderId, lhsAttr, lhsDataProviderId);
		}

		// Find DataProvider for RHS attribute
		int rhsDataProviderId = this.attribute2dataProvider[rhsAttr];
		ActorRef<DataProvider.Message> rhsDataProvider = this.dataProviders.get(rhsDataProviderId);

		// Send task to worker
		this.dependencyWorkers.get(dependencyWorkerId)
				.tell(new DependencyWorker.ValidateTaskMessage(lhsAttr, rhsAttr, rhsDataProvider));
		this.dependencyWorkersActive.set(dependencyWorkerId, true);

		this.getContext().getLog().debug("Assigned task ({} ⊆ {}?) to DependencyWorker-{} (local LHS)", lhsAttr,
				rhsAttr, dependencyWorkerId);
	}

	/**
	 * Check if a task can be pruned via bidirectional transitivity pruning.
	 * Since DependencyWorker checks both directions (lhs ⊆ rhs AND rhs ⊆ lhs),
	 * we can only prune the entire task if BOTH directions can be inferred via
	 * transitivity.
	 * 
	 * @param lhsAttr First attribute in the task
	 * @param rhsAttr Second attribute in the task
	 * @return true if both lhs ⊆ rhs AND rhs ⊆ lhs can be inferred via transitivity
	 */
	private boolean canPruneByTransitivity(int lhsAttr, int rhsAttr) {
		// Check if lhs ⊆ rhs can be pruned
		boolean canPruneLhsToRhs = canPruneDirection(lhsAttr, rhsAttr);

		// Check if rhs ⊆ lhs can be pruned
		boolean canPruneRhsToLhs = canPruneDirection(rhsAttr, lhsAttr);

		// Only prune the entire task if BOTH directions can be pruned
		// This ensures we don't skip validation when one direction is still unknown
		return canPruneLhsToRhs && canPruneRhsToLhs;
	}

	/**
	 * Check if a specific direction (from ⊆ to) can be pruned via transitivity.
	 * Returns true if there exists an intermediate attribute mid where:
	 * - from ⊆ mid (already discovered)
	 * - mid ⊆ to (already discovered)
	 * This implies from ⊆ to by transitivity.
	 */
	private boolean canPruneDirection(int from, int to) {
		Set<Integer> fromSuccessors = this.indGraph.get(from);
		if (fromSuccessors == null || fromSuccessors.isEmpty()) {
			return false; // No known successors of 'from'
		}

		// Check if any successor of 'from' is also a predecessor of 'to'
		for (int mid : fromSuccessors) {
			Set<Integer> midSuccessors = this.indGraph.get(mid);
			if (midSuccessors != null && midSuccessors.contains(to)) {
				// Found transitivity: from ⊆ mid ⊆ to
				return true;
			}
		}

		return false;
	}

	// * DEBUG OUTPUT * //
	/**
	 * Output comprehensive debug information about DependencyMiner's current state
	 */
	private void outputDebugInformation() {
		this.getContext().getLog().info("========== DEPENDENCY MINER DEBUG INFORMATION ==========");

		// Log file and attribute information
		this.getContext().getLog().info("Input Files: {}", Arrays.toString(this.inputFiles));
		this.getContext().getLog().info("Total Attributes: {}",
				this.attribute2name != null ? this.attribute2name.length : 0);

		// Log attribute details
		if (this.attribute2name != null) {
			for (int i = 0; i < this.attribute2name.length; i++) {
				this.getContext().getLog().info("  Attribute {}: {} from file {}",
						i, this.attribute2name[i], this.attribute2file[i].getName());
			}
		}

		// Log DataProvider information
		this.getContext().getLog().info("Total DataProviders: {}", this.dataProviders.size());
		for (int dpId = 0; dpId < this.dataProviders.size(); dpId++) {
			IntList attributeIds = new IntArrayList();
			if (this.attribute2dataProvider != null) {
				for (int attribute = 0; attribute < this.attribute2dataProvider.length; attribute++) {
					if (this.attribute2dataProvider[attribute] == dpId) {
						attributeIds.add(attribute);
					}
				}
			}
			this.getContext().getLog().info("  DataProvider-{}: {} attributes -> {}",
					dpId, attributeIds.size(), attributeIds);
		}

		// Log DependencyWorker information
		this.getContext().getLog().info("Total DependencyWorkers: {}", this.dependencyWorkers.size());
		for (int workerId = 0; workerId < this.dependencyWorkers.size(); workerId++) {
			Integer localDataProviderId = this.worker2dataProvider.get(workerId);
			this.getContext().getLog().info("DependencyWorker-{}: co-located with DataProvider-{}",
					workerId, localDataProviderId);
		}

		// Log co-location mapping
		this.getContext().getLog().info("DataProvider to Workers co-location mapping:");
		for (Map.Entry<Integer, List<Integer>> entry : this.dataProvider2workers.entrySet()) {
			this.getContext().getLog().info("  DataProvider-{} -> DependencyWorkers: {}",
					entry.getKey(), entry.getValue());
		}

		this.getContext().getLog().info("======================================================");
	}

	/**
	 * Send debug information to each DataProvider
	 */
	private void requestDebugInfoFromDataProviders() {
		this.getContext().getLog().info("Sending debug information to all DataProviders...");

		for (int dpId = 0; dpId < this.dataProviders.size(); dpId++) {
			// Collect attributes for this DataProvider
			IntList attributeIds = new IntArrayList();
			List<String> attributeNames = new ArrayList<>();

			if (this.attribute2dataProvider != null) {
				for (int attribute = 0; attribute < this.attribute2dataProvider.length; attribute++) {
					if (this.attribute2dataProvider[attribute] == dpId) {
						attributeIds.add(attribute);
						attributeNames.add(this.attribute2name[attribute]);
					}
				}
			}

			// Send debug message
			DataProvider.DataInfoDebugMessage debugMsg = new DataProvider.DataInfoDebugMessage(
					dpId,
					attributeIds.toIntArray(),
					attributeNames.toArray(new String[0]));

			this.dataProviders.get(dpId).tell(debugMsg);
			this.getContext().getLog().info("Sent debug info to DataProvider-{}", dpId);
		}
	}

}