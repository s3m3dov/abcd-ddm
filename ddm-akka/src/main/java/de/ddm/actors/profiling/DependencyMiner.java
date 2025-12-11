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
		this.pendingDataProviderRegistrations = new LinkedList<>();
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
		this.dependencyWorkersOverloaded = new BooleanArrayList();

		this.dataProvider2workers = new HashMap<>();

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
	private final BooleanList dependencyWorkersOverloaded;

	// Co-location tracking: maps dataProviderId to list of workerIds on same node
	private final Map<Integer, List<Integer>> dataProvider2workers;

	// Attribute mappings
	private HashMap<File, int[]> file2attribute;
	private File[] attribute2file;
	private String[] attribute2name;
	private int[] attribute2worker;
	private int[] attribute2dataProvider; // Maps attribute to DataProvider ID

	// Pending messages
	private final List<BatchMessage> pendingBatchMessages;
	private final List<DataProviderRegistrationMessage> pendingDataProviderRegistrations;
	private final List<DependencyWorkerRegistrationMessage> pendingWorkerRegistrations;

	// Validation phase state
	private boolean[][] candidateMatrix;
	private List<IntList> worker2attributes;

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
		if (this.dependencyWorkers.isEmpty())
			return this;

		// Map data providers to attributes and start reading
		this.mapAttributesToDataProviders();
		this.startReading();

		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		this.getContext().getLog().info("BatchMessage! {} active data providers",
				this.dataProvidersActive.stream().filter(b -> b).count());

		// Close the reading process for this file if the last line was read
		if (message.getBatch().length == 0) {
			this.inputReaderFinished.set(message.getReaderId(), true);

			if (this.isValidationPhase()) {
				// Log the result of the data reading process
				this.getContext().getLog()
						.info("Reading data completed in " + (System.currentTimeMillis() - this.startTime) + "ms");
				for (int attribute = 0; attribute < this.attribute2worker.length; attribute++)
					this.getContext().getLog()
							.info("Attribute " + attribute + " is on worker " + this.attribute2worker[attribute]);

				// Attempt to initiate the validation phase
				this.initiateValidationPhase();
			}
			return this;
		}

		// Wait for more workers to join and steal if any number of workers is
		// overloaded
		if (this.dataProvidersOverloaded.stream().anyMatch(b -> b)) {
			this.pendingBatchMessages.add(message);
			return this;
		}

		// Wait for pending registrations to complete
		if (!this.pendingDataProviderRegistrations.isEmpty() || !this.pendingWorkerRegistrations.isEmpty()) {
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

			this.dataProviders.get(dataProviderId)
					.tell(new DataProvider.DataMessage(attributeId, columns[i], numColumnsForThisDataProvider));
			this.dataProvidersActive.set(dataProviderId, true);
		}

		// Read the next batch from the input reader whose batch we just forwarded
		this.inputReaders.get(message.getReaderId())
				.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf(), batchSize));

		return this;
	}

	private Behavior<Message> handle(StatusMessage message) {
		this.getContext().getLog().info("StatusMessage! {} active DataProviders",
				this.dataProvidersActive.stream().filter(b -> b).count());

		this.dataProvidersActive.set(message.getDataProviderId(), message.isActive());
		this.dataProvidersOverloaded.set(message.getDataProviderId(), message.isOverloaded());

		// Check if we are already in validation phase; in that case, StatusMessages are
		// irrelevant, but we might still
		// need to initiate the validation phase, if this was the last active
		// DataProvider
		if (this.isValidationPhase())
			this.initiateValidationPhase();

		// Check all pending registration messages if they can now be processed
		if (!this.pendingDataProviderRegistrations.isEmpty()) {
			for (DataProviderRegistrationMessage registrationMessage : this.pendingDataProviderRegistrations)
				this.getContext().getSelf().tell(registrationMessage);
			this.pendingDataProviderRegistrations.clear();
		}
		if (!this.pendingWorkerRegistrations.isEmpty()) {
			for (DependencyWorkerRegistrationMessage registrationMessage : this.pendingWorkerRegistrations)
				this.getContext().getSelf().tell(registrationMessage);
			this.pendingWorkerRegistrations.clear();
			return this;
		}

		// Check all pending batch messages if they can now be processed (if there are
		// any)
		for (BatchMessage batchMessage : this.pendingBatchMessages)
			this.getContext().getSelf().tell(batchMessage);
		this.pendingBatchMessages.clear();
		return this;
	}

	private Behavior<Message> handle(DataProviderRegistrationMessage message) {
		this.getContext().getLog().info("DataProviderRegistrationMessage received");

		ActorRef<DataProvider.Message> dataProvider = message.getDataProvider();
		ActorRef<LargeMessageProxy.Message> dataProviderLMP = message.getDataProviderLMP();

		// Ignore if already registered
		if (this.dataProviders.contains(dataProvider))
			return this;

		// Register the DataProvider
		this.registerNewDataProvider(dataProvider, dataProviderLMP);

		// If we're in batch phase and this is the first DataProvider, start reading
		if (this.isBatchPhase() && this.dataProviders.size() == 1) {
			this.mapAttributesToDataProviders();
			this.startReading();
			return this;
		}

		// If in batch phase and all DataProviders are idle, trigger stealing
		if (this.isBatchPhase() && this.dataProvidersActive.stream().noneMatch(b -> b)) {
			int dataProviderId = this.dataProviders.size() - 1;
			this.stealAttributesTo(dataProvider, dataProviderId, dataProviderLMP);
			return this;
		}

		// If in validation phase, reject (too late to join)
		if (this.isValidationPhase())
			return this;

		// Otherwise, add to pending
		this.pendingDataProviderRegistrations.add(message);
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

		// Register the worker with its co-located DataProvider
		this.registerNewWorker(dependencyWorker, dependencyWorkerLMP, localDataProvider);

		// Worker registration doesn't trigger any special actions
		// Workers are passive during loading phase
		return this;
	}

	private Behavior<Message> handle(ResultMessage message) {
		this.getContext().getLog().info("ResultMessage! {} INDs from worker {}", message.getLhss().length,
				message.getWorkerId());

		// Create Inclusion Dependency objects from the results
		List<InclusionDependency> inds = new ArrayList<>(message.getLhss().length);
		for (int i = 0; i < message.getLhss().length; i++) {
			String[] dependentAttributes = { this.attribute2name[message.getLhss()[i]] };
			String[] referencedAttributes = { this.attribute2name[message.getRhss()[i]] };
			inds.add(new InclusionDependency(this.attribute2file[message.getLhss()[i]], dependentAttributes,
					this.attribute2file[message.getRhss()[i]], referencedAttributes));
		}

		// Send Inclusion Dependency objects to the result collector
		this.resultCollector.tell(new ResultCollector.ResultMessage(inds));

		// Find more work for the dependency worker
		int workerId = message.getWorkerId();
		for (int attribute : this.worker2attributes.get(workerId)) {
			for (int otherAttribute = 0; otherAttribute < this.attribute2name.length; otherAttribute++) {
				if (!this.candidateMatrix[attribute][otherAttribute]) {
					// Collect all the worker's attributes that still require validation with the
					// current other attribute and mark them validated
					IntList untestedAttributes = new IntArrayList(this.worker2attributes.get(workerId).size());
					for (int untestedAttributeCandidate : this.worker2attributes.get(workerId)) {
						if (!this.candidateMatrix[otherAttribute][untestedAttributeCandidate]) {
							this.candidateMatrix[otherAttribute][untestedAttributeCandidate] = true;
							this.candidateMatrix[untestedAttributeCandidate][otherAttribute] = true;
							untestedAttributes.add(untestedAttributeCandidate);
						}
					}

					// Tell the owner of the other attribute to send that attribute's column to the
					// worker for validation
					int otherWorkerId = this.attribute2worker[otherAttribute];
					this.dependencyWorkers.get(otherWorkerId).tell(new DependencyWorker.ShareForValidationMessage(
							otherAttribute, untestedAttributes.toIntArray(), this.dependencyWorkers.get(workerId),
							workerId, this.dependencyWorkerLMPs.get(workerId)));

					return this;
				}
			}
		}

		// Set the worker to inactive, because no further validation work could be found
		// for its attributes
		this.dependencyWorkersActive.set(workerId, false);

		// End the program when
		// all INDs have been checked, which means all workers are inactive now
		if (this.dependencyWorkersActive.stream().noneMatch(b -> b)) {
			this.resultCollector.tell(new ResultCollector.FinalizeMessage());
			long discoveryTime = System.currentTimeMillis() - this.startTime;
			this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
		}
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

		this.getContext().getLog().info("Registered DataProvider {}", dataProviderId);
	}

    private void registerNewWorker(ActorRef<DependencyWorker.Message> dependencyWorker,
			ActorRef<LargeMessageProxy.Message> dependencyWorkerLMP, ActorRef<DataProvider.Message> localDataProvider) {
		this.dependencyWorkers.add(dependencyWorker);
		this.dependencyWorkerLMPs.add(dependencyWorkerLMP);
		this.getContext().watch(dependencyWorker);
		this.dependencyWorkersActive.add(false);
		this.dependencyWorkersOverloaded.add(false);

		// Track co-location: add workerId to DataProvider's worker list
		int workerId = this.dependencyWorkers.size() - 1;
		int dataProviderId = this.dataProviders.indexOf(localDataProvider);
		this.dataProvider2workers.computeIfAbsent(dataProviderId, k -> new ArrayList<>()).add(workerId);

		this.getContext().getLog().info("Registered Worker {} co-located with DataProvider {}",
				workerId, dataProviderId);
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
					.tell(new DataProvider.SetIdAndAttributesMessage(dataProviderId, attributes.toArray(new int[0])));
			this.dataProvidersActive.set(dataProviderId, true);
		}
	}

	// * STEALING * //
	private void stealAttributesTo(ActorRef<DataProvider.Message> dataProvider, int dataProviderId,
			ActorRef<LargeMessageProxy.Message> dataProviderLMP) {
		// Find attributes to steal; we aim to steal evenly from the DataProviders with
		// the
		// most partitions (this is a very simple stealing approach)
		IntSet attributesToSteal = new IntAVLTreeSet();
		int numDataProviders = this.dataProviders.size();
		int numAttributes = this.attribute2name.length;
		int numAttributesToSteal = (int) Math.floor((double) numAttributes / numDataProviders);

		int[] attributesPerDataProviderCounts = new int[numDataProviders];
		for (int dpId : this.attribute2dataProvider)
			attributesPerDataProviderCounts[dpId] += 1;

		Random random = new Random();
		while (attributesToSteal.size() < numAttributesToSteal) {
			// Find DataProvider with the currently most partitions
			int dataProviderWithHighestCount = 0;
			int highestCount = attributesPerDataProviderCounts[0];
			for (int i = 1; i < numDataProviders; i++) {
				if (attributesPerDataProviderCounts[i] > highestCount) {
					dataProviderWithHighestCount = i;
					highestCount = attributesPerDataProviderCounts[i];
				}
			}

			// Steal one random partition from the DataProvider with the currently most
			// partitions
			int randomPartitionNumber = random.nextInt(highestCount) + 1;
			for (int i = 0; i < this.attribute2dataProvider.length; i++) {
				if ((this.attribute2dataProvider[i] == dataProviderWithHighestCount)
						&& (!attributesToSteal.contains(i))) {
					randomPartitionNumber--;
					if (randomPartitionNumber == 0) {
						attributesToSteal.add(i);
						break;
					}
				}
			}
			attributesPerDataProviderCounts[dataProviderWithHighestCount] -= 1;
		}

		// Collect the stolen attributes for every DataProvider that needs to send
		// stolen
		// attributes to the new DataProvider and reassign stolen attributes
		Int2ObjectMap<IntArrayList> dataProvider2stolenAttributes = new Int2ObjectArrayMap<>(this.dataProviders.size());
		for (int attribute : attributesToSteal) {
			int sourceDataProviderId = this.attribute2dataProvider[attribute];
			if (!dataProvider2stolenAttributes.containsKey(sourceDataProviderId)) {
				dataProvider2stolenAttributes.put(sourceDataProviderId, new IntArrayList());

			}
			dataProvider2stolenAttributes.get(sourceDataProviderId).add(attribute);
			this.attribute2dataProvider[attribute] = dataProviderId;
		}

		// Send the MoveDataMessages to start the stealing process
		for (Int2ObjectMap.Entry<IntArrayList> entry : dataProvider2stolenAttributes.int2ObjectEntrySet()) {
			int[] attributes = entry.getValue().toArray(new int[0]);
			int sourceDataProviderId = entry.getIntKey();
			DataProvider.MoveDataMessage moveDataMessage = new DataProvider.MoveDataMessage(
					attributes, dataProviderId, dataProvider, dataProviderLMP, dataProvider2stolenAttributes.size());
			this.dataProviders.get(sourceDataProviderId).tell(moveDataMessage);
			this.dataProvidersActive.set(sourceDataProviderId, true);
		}
		this.dataProvidersActive.set(dataProviderId, true);
	}

	// * VALIDATION PHASE * //
	private void initiateValidationPhase() {
		int numAttributes = this.attribute2name.length;
		int numWorkers = this.dependencyWorkers.size();

		// Check that all workers are inactive; otherwise, we wait for a status update
		// of a still active worker to initiate the validation phase
		if (this.dependencyWorkersActive.stream().anyMatch(b -> b))
			return;

		// Create an IND candidate matrix to keep track of the candidate validations
		this.candidateMatrix = new boolean[numAttributes][];
		for (int i = 0; i < numAttributes; i++)
			this.candidateMatrix[i] = new boolean[numAttributes];

		// Create a worker-to-attributes mapping to support the initial candidate
		// generation
		this.worker2attributes = new ArrayList<>(numWorkers);
		for (int workerId = 0; workerId < numWorkers; workerId++)
			this.worker2attributes.add(new IntArrayList((int) Math.ceil((float) numAttributes / (float) numWorkers)));
		for (int attribute = 0; attribute < numAttributes; attribute++)
			this.worker2attributes.get(this.attribute2worker[attribute]).add(attribute);

		// Send a ValidateLocalCandidatesMessage to all workers and mark their
		// candidates as validated. The workers will
		// then receive further validation tasks via work pulling, i.e., whenever they
		// report results.
		for (int workerId = 0; workerId < numWorkers; workerId++) {
			this.dependencyWorkers.get(workerId).tell(new DependencyWorker.ValidateLocalCandidatesMessage());
			IntList workerAttributes = this.worker2attributes.get(workerId);
			for (int i = 0; i < workerAttributes.size(); i++) {
				for (int j = i; j < workerAttributes.size(); j++) {
					this.candidateMatrix[workerAttributes.getInt(i)][workerAttributes.getInt(j)] = true;
					this.candidateMatrix[workerAttributes.getInt(j)][workerAttributes.getInt(i)] = true;
				}
			}

			this.dependencyWorkersActive.set(workerId, true);
		}
	}

}