package org.apache.hadoop.yarn.server.resourcemanager.scheduler.adaptive;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.LifecycleEvent;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.ContainersAndNMTokensAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

@LimitedPrivate("yarn")
@Unstable
@SuppressWarnings("unchecked")
public class AdaptiveScheduler extends AbstractYarnScheduler<FiCaSchedulerApp, FiCaSchedulerNode>
		implements Configurable {

	private static final Log LOG = LogFactory.getLog(AdaptiveScheduler.class);
	private Configuration conf;
	private boolean usePortForNodeName;
	private AdaptiveQueue queue;
	private final String DEFAULT_QUEUE_NAME = "default";
	private static final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
	private Resource usedResources = recordFactory.newRecordInstance(Resource.class);
	private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

	// migration code from the MRv1
	// private volatile boolean running = false;
	// private volatile boolean pendingEvent = false;
	// private boolean initialized = false;
	// private Map<FiCaSchedulerApp, JobInfo> jobs = new
	// HashMap<FiCaSchedulerApp, JobInfo>();
	// private Map<String, JobInfo> jobsByID = new HashMap<String, JobInfo>();
	// private ClusterOutline cluster = new ClusterOutline();
	// protected volatile static long INTERVAL = 10000;
	// private int LOOP = 3;
	// private float UTILIZATION = 100.0f;

	/**
	 * added by 2015-7-9
	 * 
	 * @author ericson
	 * 
	 */
	// // preserve the resources on each node, C
	// private Map<NodeId, Resource> allResourceArray = new
	// ConcurrentSkipListMap<NodeId, Resource>();
	// // preserve the weight of each resource item
	// private double[] resourceWeights;
	// // preserve all the containers in the cluster with its resources, R
	// private Map<String, Resource> containerByResources = new
	// ConcurrentSkipListMap<String, Resource>();
	// // preserve all the AppMasterContainer to first launch
	// private List<String> appMasters = new ArrayList<String>();
	// // preserve the container index to assign to the node
	// private Map<String, List<ContainerInfo>> willAssignContainers = new
	// HashMap<String, List<ContainerInfo>>();
	// // preserve the containerId with the applicationI
	// private Map<String, List<String>> applicationContainers = new
	// HashMap<String, List<String>>();
	// // preserve the container info
	// private List<ApplicationRequestInfo> applicationInfoQueue;
	//
	// // preserve the applicationInfo
	// private Map<ApplicationId, ApplicationInfo> applicationinfos;

	/**
	 * added by ericson at 2015-07-15, the newly version
	 */
	// preserve all the job profiles
	private Map<ApplicationId, JobInfo> jobinfos;
	// preserve all the container profiles
	// private PriorityQueue<ContainerProfile> allContainerQueue;
	private Map<ContainerProfile, Integer> allContainers;
	// preserve the each node resources
	// private Map<NodeId, Resource> nodeResources;
	// preserve the assign containers at this nodemanager heartbeat
	private Map<ContainerProfile, Integer> assignContainersList;
	// used to the initial assignment
	private boolean initialize;
	// used to stop the initial assignment
	private boolean stopInitialize;
	// used to flag the container release
	private boolean containerRelease;
	// used to node change
	private boolean noderesourceChange;
	// used to begin with real-time assignment
	private boolean beginRealtime;
	// the weights of the resources
	private double[] weightResources;

	private ResourceCollector collector;
	private JobResourceCollector jobcollector;

	private class ContainerProfileComparator implements Comparator<ContainerProfile> {

		@Override
		public int compare(ContainerProfile o1, ContainerProfile o2) {
			// TODO Auto-generated method stub
			int priority1 = o1.getContainerRequest().getPriority().getPriority();
			int priority2 = o2.getContainerRequest().getPriority().getPriority();
			if (priority1 != priority2) {
				if (priority1 == 0)
					return 1;
				if (priority2 == 0)
					return -1;
				return Integer.compare(o2.getContainerRequest().getPriority().getPriority(),
						o1.getContainerRequest().getPriority().getPriority());
			} else {
				return o1.getApplicationid().toString().compareTo(o2.getApplicationid().toString());
			}
		}

	}

	private class ApplicationRequestComparator implements Comparator<ApplicationRequestInfo> {

		@Override
		public int compare(ApplicationRequestInfo o1, ApplicationRequestInfo o2) {
			// TODO Auto-generated method stub
			if (o1.getRequest().getPriority().getPriority() != o2.getRequest().getPriority().getPriority()) {
				return Integer.compare(o1.getRequest().getPriority().getPriority(),
						o2.getRequest().getPriority().getPriority());
			}
			if (!o1.getRequest().getCapability().equals(o2.getRequest().getCapability())) {
				return o1.getRequest().getCapability().compareTo(o2.getRequest().getCapability());
			}
			String applicationString1 = o1.getApplicationId().toString();
			String applicationString2 = o2.getApplicationId().toString();
			return applicationString1.compareTo(applicationString2);
		}

	}

	private class ContainerInfoComparator implements Comparator<ContainerInfo> {

		@Override
		public int compare(ContainerInfo o1, ContainerInfo o2) {
			// TODO Auto-generated method stub
			if (o1.getRequest().getPriority().getPriority() != o2.getRequest().getPriority().getPriority()) {
				return Integer.compare(o1.getRequest().getPriority().getPriority(),
						o2.getRequest().getPriority().getPriority());
			}
			ResourceRequest r1 = o1.getRequest();
			ResourceRequest r2 = o2.getRequest();
			return r1.getCapability().compareTo(r2.getCapability());
		}
	}

	public class AdaptiveQueue implements Queue {

		private String queueName = "default";
		private QueueMetrics metrics;
		private ActiveUsersManager activeUsersManager;

		public AdaptiveQueue(String queueName, QueueMetrics metrics) {
			this.queueName = queueName;
			this.metrics = metrics;
			this.activeUsersManager = new ActiveUsersManager(metrics);
		}

		public AdaptiveQueue(String queueName, Configuration conf) {
			this.queueName = queueName;
			this.metrics = QueueMetrics.forQueue(queueName, null, false, conf);
			this.activeUsersManager = new ActiveUsersManager(this.metrics);
		}

		@Override
		public String getQueueName() {
			// TODO Auto-generated method stub
			return queueName;
		}

		@Override
		public QueueMetrics getMetrics() {
			// TODO Auto-generated method stub
			return metrics;
		}

		@Override
		public QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive) {
			// TODO Auto-generated method stub
			QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
			queueInfo.setQueueName(this.getQueueName());
			queueInfo.setCapacity(1.0f);
			if (clusterResource.getMemory() == 0) {
				queueInfo.setCurrentCapacity(0.0f);
			} else {
				queueInfo.setCurrentCapacity((float) usedResources.getMemory() / clusterResource.getMemory());
			}
			queueInfo.setMaximumCapacity(1.0f);
			queueInfo.setChildQueues(new ArrayList<QueueInfo>());
			queueInfo.setQueueState(QueueState.RUNNING);
			return queueInfo;
		}

		@Override
		public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
			// TODO Auto-generated method stub
			QueueUserACLInfo queueUserAclInfo = recordFactory.newRecordInstance(QueueUserACLInfo.class);
			queueUserAclInfo.setQueueName(this.queueName);
			queueUserAclInfo.setUserAcls(Arrays.asList(QueueACL.values()));
			return Collections.singletonList(queueUserAclInfo);
		}

		public Map<QueueACL, AccessControlList> getQueueAcls() {
			Map<QueueACL, AccessControlList> acls = new HashMap<QueueACL, AccessControlList>();
			for (QueueACL acl : QueueACL.values()) {
				acls.put(acl, new AccessControlList("*"));
			}
			return acls;
		}

		@Override
		public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
			// TODO Auto-generated method stub
			return getQueueAcls().get(acl).isUserAllowed(user);
		}

		@Override
		public ActiveUsersManager getActiveUsersManager() {
			// TODO Auto-generated method stub
			return this.activeUsersManager;
		}

		@Override
		public void recoverContainer(Resource clusterResource, SchedulerApplicationAttempt schedulerAttempt,
				RMContainer rmContainer) {
			// TODO Auto-generated method stub
			if (rmContainer.getState().equals(RMContainerState.COMPLETED))
				return;
			increaseUsedResources(rmContainer);
			updateAppHeadRoom(schedulerAttempt);
			updateAvailableResourcesMetrics();
		}

	}

	@Override
	public synchronized List<Container> getTransferredContainers(ApplicationAttemptId currentAttempt) {
		// TODO Auto-generated method stub
		return super.getTransferredContainers(currentAttempt);
	}

	@Override
	public Map<ApplicationId, SchedulerApplication<FiCaSchedulerApp>> getSchedulerApplications() {
		// TODO Auto-generated method stub
		return super.getSchedulerApplications();
	}

	@Override
	public Resource getClusterResource() {
		// TODO Auto-generated method stub
		return super.getClusterResource();
	}

	@Override
	public Resource getMinimumResourceCapability() {
		// TODO Auto-generated method stub
		return super.getMinimumResourceCapability();
	}

	@Override
	public Resource getMaximumResourceCapability() {
		// TODO Auto-generated method stub
		return super.getMaximumResourceCapability();
	}

	@Override
	public FiCaSchedulerApp getApplicationAttempt(ApplicationAttemptId applicationAttemptId) {
		// TODO Auto-generated method stub
		return super.getApplicationAttempt(applicationAttemptId);
	}

	@Override
	public SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId appAttemptId) {
		// TODO Auto-generated method stub
		return super.getSchedulerAppInfo(appAttemptId);
	}

	@Override
	public ApplicationResourceUsageReport getAppResourceUsageReport(ApplicationAttemptId appAttemptId) {
		// TODO Auto-generated method stub
		return super.getAppResourceUsageReport(appAttemptId);
	}

	@Override
	public FiCaSchedulerApp getCurrentAttemptForContainer(ContainerId containerId) {
		// TODO Auto-generated method stub
		return super.getCurrentAttemptForContainer(containerId);
	}

	@Override
	public RMContainer getRMContainer(ContainerId containerId) {
		// TODO Auto-generated method stub
		FiCaSchedulerApp attempt = getCurrentAttemptForContainer(containerId);
		return (attempt == null) ? null : attempt.getRMContainer(containerId);
	}

	@Override
	public SchedulerNodeReport getNodeReport(NodeId nodeId) {
		// TODO Auto-generated method stub
		return super.getNodeReport(nodeId);
	}

	@Override
	public String moveApplication(ApplicationId appId, String newQueue) throws YarnException {
		// TODO Auto-generated method stub
		return super.moveApplication(appId, newQueue);
	}

	@Override
	public synchronized void recoverContainersOnNode(List<NMContainerStatus> containerReports, RMNode nm) {
		// TODO Auto-generated method stub
		super.recoverContainersOnNode(containerReports, nm);
	}

	@Override
	protected void recoverResourceRequestForContainer(RMContainer rmContainer) {
		// TODO Auto-generated method stub
		super.recoverResourceRequestForContainer(rmContainer);
	}

	@Override
	public SchedulerNode getSchedulerNode(NodeId nodeId) {
		// TODO Auto-generated method stub
		return super.getSchedulerNode(nodeId);
	}

	@Override
	public synchronized STATE getFailureState() {
		// TODO Auto-generated method stub
		return super.getFailureState();
	}

	@Override
	protected void setConfig(Configuration conf) {
		// TODO Auto-generated method stub
		super.setConfig(conf);
	}

	@Override
	public void init(Configuration conf) {
		// TODO Auto-generated method stub
		super.init(conf);
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		super.start();
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		super.stop();
	}

	@Override
	protected void serviceInit(Configuration conf) throws Exception {
		// TODO Auto-generated method stub
		initScheduler(conf);
		super.serviceInit(conf);
		outputRMContext();// get the rmContext information
	}

	private void outputRMContext() {

	}

	private void initScheduler(Configuration conf) {
		validateConf(conf);
		this.applications = new ConcurrentSkipListMap<ApplicationId, SchedulerApplication<FiCaSchedulerApp>>();
		this.minimumAllocation = Resources.createResource(
				conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
						YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB),
				conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
						YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES));
		this.maximumAllocation = Resources.createResource(
				conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
						YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB),
				conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
						YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES));
		this.usePortForNodeName = conf.getBoolean(YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
		this.queue = new AdaptiveQueue(DEFAULT_QUEUE_NAME, this.conf);

		// this.applicationInfoQueue = new ArrayList<ApplicationRequestInfo>();
		// this.applicationinfos = new HashMap<ApplicationId,
		// ApplicationInfo>();
		// this.resourceWeights = new double[2];
		// resourceWeights[0] = 0.5;
		// resourceWeights[1] = 0.5;

		/**
		 * added by ericson at 2015-07-15
		 */
		jobinfos = new HashMap<ApplicationId, JobInfo>();
		// this.allContainerQueue = new PriorityQueue<ContainerProfile>(20, new
		// ContainerProfileComparator());
		allContainers = new ConcurrentSkipListMap<ContainerProfile, Integer>(new ContainerProfileComparator());
		assignContainersList = new ConcurrentSkipListMap<ContainerProfile, Integer>(new ContainerProfileComparator());
		// nodeResources = new HashMap<NodeId, Resource>();
		initialize = false;
		stopInitialize = true;
		containerRelease = false;
		noderesourceChange = false;
		beginRealtime = false;
		weightResources = new double[2];
		weightResources[0] = 0.8;
		weightResources[1] = 0.2;

		Map<String, String> m = System.getenv();
		if (m.containsKey("HADOOP_HOME")) {
			collector = new ResourceCollector(m.get("HADOOP_HOME") + "/logs/totalresource");
			collector.start();
			jobcollector = new JobResourceCollector(m.get("HADOOP_HOME") + "/logs/jobresource");
			jobcollector.start();
		}

		// this.INTERVAL = this.conf.getInt("yarn.scheduler.adaptive.interval",
		// 10000);
		// this.LOOP = this.conf.getInt("yarn.scheduler.adaptive.loop", 3);
		// this.UTILIZATION =
		// this.conf.getInt("yarn.scheduler.adaptive.utilization", 100);
		// this.initialized = true;
		// this.running = true;
		// this.pendingEvent = false;
		// This thread will be the most important part of the scheduler
		// new AdaptivePlaceThread().start();
	}

	class JobResourceCollector extends Thread {
		private BufferedWriter writer = null;
		private boolean flag = true;

		public void setFlag(boolean flag) {
			this.flag = flag;
		}

		public JobResourceCollector(String filename) {
			try {
				File file = new File(filename);
				if (file.exists()) {
					file.delete();
				}
				file.createNewFile();
				FileWriter fw = new FileWriter(file, false);
				writer = new BufferedWriter(fw);
			} catch (Exception e) {
				LOG.info("init error:" + e.getMessage());
			}
		}

		private double getApplicationTotalMem(FiCaSchedulerApp app) {
			double totalmem = 0;
			Iterator<RMContainer> iters = app.getLiveContainers().iterator();
			while (iters.hasNext()) {
				double temp = iters.next().getAllocatedResource().getMemory();
				totalmem += temp;
			}
			return totalmem / minimumAllocation.getMemory();
		}

		private double getApplicationTotalCpu(FiCaSchedulerApp app) {
			double totalcpu = 0;
			Iterator<RMContainer> iters = app.getLiveContainers().iterator();
			while (iters.hasNext()) {
				double temp = iters.next().getAllocatedResource().getVirtualCores();
				totalcpu += temp;
			}
			return totalcpu / minimumAllocation.getVirtualCores();
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			try {
				while (flag) {
					// totalmemory = clusterResource.getMemory() /
					// minimumAllocation.getMemory();
					// totalcpucores = clusterResource.getVirtualCores() /
					// minimumAllocation.getVirtualCores();
					if (!applications.isEmpty()
							&& (usedResources.getMemory() > 0 || usedResources.getVirtualCores() > 0)) {
						double[] mems = new double[applications.size()];
						double[] cpus = new double[applications.size()];
						String[] appids = new String[mems.length];
						int count = 0;
						for (Map.Entry<ApplicationId, SchedulerApplication<FiCaSchedulerApp>> entry : applications
								.entrySet()) {
							FiCaSchedulerApp appAttempt = entry.getValue().getCurrentAppAttempt();
							mems[count] = getApplicationTotalMem(appAttempt);
							cpus[count] = getApplicationTotalCpu(appAttempt);
							appids[count] = String.valueOf(entry.getKey().getId());
							count++;
						}
						String result = "";
						for (int i = 0; i < mems.length; i++) {
							result += appids[i] + ":" + mems[i] + ",";
						}
						writer.write(System.nanoTime() + "," + result.substring(0, result.lastIndexOf(',')));
						writer.newLine();
						writer.flush();
						Thread.sleep(300);
					}
				}
			} catch (Exception e) {
				LOG.info("----------error message:" + e.getMessage());
			} finally {
				try {
					writer.close();
				} catch (Exception e) {
					LOG.info("-------------close error:" + e.getMessage());
				}
			}
		}
	}

	class ResourceCollector extends Thread {
		private boolean flag = true;
		private BufferedWriter writer = null;
		private int count = 0;
		private double totalmemory;
		private double totalcpucores;

		public void setFlag(boolean flag) {
			this.flag = flag;
		}

		public ResourceCollector(String filename) {
			try {
				File file = new File(filename);
				if (file.exists()) {
					file.delete();
				}
				file.createNewFile();
				FileWriter fw = new FileWriter(filename, false);
				writer = new BufferedWriter(fw);
			} catch (Exception e) {
				LOG.info("init error:" + e.getMessage());
			}
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			try {
				while (flag) {
					totalmemory = clusterResource.getMemory() / minimumAllocation.getMemory();
					totalcpucores = clusterResource.getVirtualCores() / minimumAllocation.getVirtualCores();
					if (!applications.isEmpty()
							&& (usedResources.getMemory() > 0 || usedResources.getVirtualCores() > 0) && totalmemory > 0
							&& totalcpucores > 0) {
						count++;
						double memory = usedResources.getMemory() / minimumAllocation.getMemory();
						double cpu = usedResources.getVirtualCores() / minimumAllocation.getVirtualCores();
						writer.write(count + "," + (memory / totalmemory) + "," + (cpu / totalcpucores));
						writer.newLine();
						writer.flush();
						Thread.sleep(300);
					}
				}
			} catch (Exception e) {
				LOG.info("error message:" + e.getMessage());
			} finally {
				try {
					writer.close();
				} catch (Exception e) {
					LOG.info(e.getMessage());
				}
			}
		}
	}

	private void validateConf(Configuration conf) {
		int minMem = conf.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
		int maxMem = conf.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);
		if (minMem <= 0 || minMem > maxMem) {
			throw new YarnRuntimeException("Error Configuration");
		}
	}

	@Override
	protected void serviceStart() throws Exception {
		// TODO Auto-generated method stub
		super.serviceStart();
	}

	@Override
	protected void serviceStop() throws Exception {
		// TODO Auto-generated method stub
		// this.running = false;
		this.initialize = false;
		this.beginRealtime = false;
		this.jobinfos.clear();
		this.allContainers.clear();
		this.assignContainersList.clear();
		// collector.setFlag(false);
		jobcollector.setFlag(false);
		super.serviceStop();
	}

	@Override
	public void registerServiceListener(ServiceStateChangeListener l) {
		// TODO Auto-generated method stub
		super.registerServiceListener(l);
	}

	@Override
	public void unregisterServiceListener(ServiceStateChangeListener l) {
		// TODO Auto-generated method stub
		super.unregisterServiceListener(l);
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return super.getName();
	}

	@Override
	public synchronized Configuration getConfig() {
		// TODO Auto-generated method stub
		return super.getConfig();
	}

	@Override
	public long getStartTime() {
		// TODO Auto-generated method stub
		return super.getStartTime();
	}

	@Override
	public synchronized List<LifecycleEvent> getLifecycleHistory() {
		// TODO Auto-generated method stub
		return super.getLifecycleHistory();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString() + ":AdaptiveScheduler";
	}

	@Override
	protected void putBlocker(String name, String details) {
		// TODO Auto-generated method stub
		super.putBlocker(name, details);
	}

	@Override
	public void removeBlocker(String name) {
		// TODO Auto-generated method stub
		super.removeBlocker(name);
	}

	@Override
	public Map<String, String> getBlockers() {
		// TODO Auto-generated method stub
		return super.getBlockers();
	}

	public AdaptiveScheduler(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}

	public AdaptiveScheduler() {
		super(AdaptiveScheduler.class.getName());
	}

	@Override
	public void setRMContext(RMContext rmContext) {
		// TODO Auto-generated method stub
		this.rmContext = rmContext;
	}

	@Override
	public void reinitialize(Configuration conf, RMContext rmContext) throws IOException {
		// TODO Auto-generated method stub
		setConf(this.conf);
		this.rmContext = rmContext;
		this.initScheduler(getConf());
	}

	@Override
	public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues, boolean recursive) throws IOException {
		// TODO Auto-generated method stub
		return this.queue.getQueueInfo(false, false);
	}

	@Override
	public List<QueueUserACLInfo> getQueueUserAclInfo() {
		// TODO Auto-generated method stub
		return this.queue.getQueueUserAclInfo(null);
	}

	@Override
	public int getNumClusterNodes() {
		// TODO Auto-generated method stub
		return this.nodes.size();
	}

	@Override
	public Allocation allocate(ApplicationAttemptId appAttemptId, List<ResourceRequest> ask, List<ContainerId> release,
			List<String> blacklistAdditions, List<String> blacklistRemovals) {
		// TODO Auto-generated method stub
		FiCaSchedulerApp application = getApplicationAttempt(appAttemptId);
		if (application == null) {
			LOG.info("Calling allocate on removed or non existant application " + appAttemptId);
			return EMPTY_ALLOCATION;
		}
		SchedulerUtils.normalizeRequests(ask, resourceCalculator, clusterResource, minimumAllocation,
				maximumAllocation);
		for (ContainerId releasedContainer : release) {
			RMContainer rmContainer = getRMContainer(releasedContainer);
			if (rmContainer == null) {
				RMAuditLogger.logFailure(application.getUser(), AuditConstants.RELEASE_CONTAINER,
						"Unauthorized access or invalid container", "AdaptiveScheduler",
						"Trying to release container not owned by app or with invalid id",
						application.getApplicationId(), releasedContainer);
			}
			containerCompleted(rmContainer,
					SchedulerUtils.createAbnormalContainerStatus(releasedContainer, SchedulerUtils.RELEASED_CONTAINER),
					RMContainerEventType.RELEASED);
		}
		synchronized (application) {
			if (application.isStopped()) {
				return EMPTY_ALLOCATION;
			}
			if (!ask.isEmpty()) {
				application.showRequests();
				application.updateResourceRequests(ask);
				application.showRequests();
				/**
				 * added by ericson at 2015-07-15
				 */
				fillAllContainers(application);
				// fillApplicationInfos(application);
			}
			application.updateBlacklist(blacklistAdditions, blacklistRemovals);
			ContainersAndNMTokensAllocation allocation = application.pullNewlyAllocatedContainersAndNMTokens();
			// assignTasks();
			return new Allocation(allocation.getContainerList(), application.getHeadroom(), null, null, null,
					allocation.getNMTokenList());
		}
	}

	/**
	 * added by ericson at 2015-07-15
	 * 
	 * @param application
	 *            application attempt
	 */
	private void fillAllContainers(FiCaSchedulerApp application) {
		synchronized (this) {
			ApplicationId applicationId = application.getApplicationId();
			if (jobinfos.containsKey(applicationId)) {
				for (Priority priority : application.getPriorities()) {
					ResourceRequest request = application.getResourceRequest(priority, ResourceRequest.ANY);
					if (request.getNumContainers() > 0) {
						this.initialize = true;
						ContainerProfile containerprofile = new ContainerProfile(applicationId,
								request.getNumContainers(), request);
						// this.allContainerQueue.offer(containerprofile);
						// this.allContainers.put(containerprofile,
						// containerprofile.getNumberOfContainer());
						if (priority.getPriority() == 0) {

							ContainerProfile appMasterProfile = this.jobinfos.get(applicationId).getAppMasterProfile();
							if (appMasterProfile == null) {

								// if
								// (assignContainersList.containsKey(containerprofile))
								// {
								// assignContainersList.put(containerprofile,
								// containerprofile.getNumberOfContainer());
								// }
								jobinfos.get(applicationId).setAppMasterNum(request.getNumContainers());
							} else {
								// to be continue
								// can use the equal function in
								// ContainerProfile and the HashCode function
								// if
								// (!allContainers.containsKey(appMasterProfile))
								// {
								// allContainers.put(containerprofile,
								// containerprofile.getContainerRequest().getNumContainers());
								// } else{
								// allContainers.put(containerprofile,
								// allContainers.get(appMasterProfile)
								// +
								// containerprofile.getContainerRequest().getNumContainers()
								// -
								// appMasterProfile.getContainerRequest().getNumContainers());
								// }
								// appMasterProfile.setContainerRequest(request);
							}
							allContainers.put(containerprofile, request.getNumContainers());
							jobinfos.get(applicationId).setAppMasterProfile(containerprofile);

						} else if (priority.getPriority() == 10) {
							LOG.info("###### add application request:" + applicationId.toString() + "->"
									+ request.toString());
							ContainerProfile reduceProfile = this.jobinfos.get(applicationId).getReduceProfile();
							if (reduceProfile == null) {
								// Set<ContainerProfile> sets =
								// allContainers.keySet();
								// if (sets.contains(containerprofile)) {
								// ContainerProfile tempprofile =
								// sets.iterator().next();
								// LOG.info("------------containerprofilehashcode:"
								// + containerprofile.hashCode());
								// LOG.info("-----------hascontainerhashcode:" +
								// tempprofile.hashCode());
								// if (tempprofile.equals(containerprofile)) {
								// LOG.info("---------------equals");
								// }
								// }

								LOG.info("---------------allContainers.size:" + allContainers.size());
								jobinfos.get(applicationId).setReduceNum(request.getNumContainers());
								// if
								// (assignContainersList.containsKey(containerprofile))
								// {
								// assignContainersList.put(containerprofile,
								// containerprofile.getNumberOfContainer());
								// }
							} else {
								// to be continue
								// if
								// (!allContainers.containsKey(reduceProfile)) {
								// allContainers.put(containerprofile,
								// containerprofile.getContainerRequest().getNumContainers());
								// } else
								// allContainers.put(containerprofile,
								// allContainers.get(reduceProfile)
								// +
								// containerprofile.getContainerRequest().getNumContainers()
								// -
								// reduceProfile.getContainerRequest().getNumContainers());
								// reduceProfile.setContainerRequest(request);
							}
							allContainers.put(containerprofile, request.getNumContainers());
							jobinfos.get(applicationId).setReduceProfile(containerprofile);
						} else if (priority.getPriority() == 20) {
							LOG.info("###### add application request:" + applicationId.toString() + "->"
									+ request.toString());
							ContainerProfile mapprofile = this.jobinfos.get(applicationId).getMapProfile();
							if (mapprofile == null) {

								// if
								// (assignContainersList.containsKey(containerprofile))
								// {
								// assignContainersList.put(containerprofile,
								// containerprofile.getNumberOfContainer());
								// }
								jobinfos.get(applicationId).setMapNum(request.getNumContainers());
							} else {
								// to be continue
								// if (!allContainers.containsKey(mapprofile)) {
								// allContainers.put(containerprofile,
								// containerprofile.getContainerRequest().getNumContainers());
								// } else
								// allContainers.put(containerprofile,
								// allContainers.get(mapprofile)
								// +
								// containerprofile.getContainerRequest().getNumContainers()
								// -
								// mapprofile.getContainerRequest().getNumContainers());
								// mapprofile.setContainerRequest(request);
							}
							allContainers.put(containerprofile, request.getNumContainers());
							jobinfos.get(applicationId).setMapProfile(containerprofile);
						} else {
							jobinfos.get(applicationId).setOther(containerprofile);
							allContainers.put(containerprofile,
									containerprofile.getContainerRequest().getNumContainers());
						}
					} else {
						if (priority.getPriority() == 0) {
							ContainerProfile profile = jobinfos.get(applicationId).getAppMasterProfile();
							allContainers.remove(profile);
						} else if (priority.getPriority() == 10) {
							ContainerProfile profile = jobinfos.get(applicationId).getReduceProfile();
							allContainers.remove(profile);
						} else if (priority.getPriority() == 20) {
							ContainerProfile profile = jobinfos.get(applicationId).getMapProfile();
							allContainers.remove(profile);
						} else {
							ContainerProfile profile = jobinfos.get(applicationId).getOther();
							allContainers.remove(profile);
						}
					}
				}
			}
		}
	}

	// private void printJobInfos() {
	// for (Map.Entry<ApplicationId, JobInfo> entry : jobinfos.entrySet()) {
	// JobInfo info = entry.getValue();
	// LOG.info("jobinfo");
	// }
	// for (Map.Entry<ContainerProfile, Integer> entry :
	// assignContainersList.entrySet()) {
	// ContainerProfile profile = entry.getKey();
	// LOG.info("profile");
	// int num = entry.getValue();
	// ResourceRequest request = profile.getContainerRequest();
	// }
	// for(Map.Entry<ContainerProfile, Integer> entry:allContainers.entrySet()){
	// ContainerProfile profile=entry.getKey();
	// LOG.info("profileget");
	// }
	// }

	// private void fillApplicationInfos(FiCaSchedulerApp application) {
	// synchronized (applicationinfos) {
	// ApplicationInfo appInfo = null;
	// if (this.applicationinfos.containsKey(application.getApplicationId())) {
	// appInfo = this.applicationinfos.get(application.getApplicationId());
	// } else {
	// appInfo = new ApplicationInfo(application.getApplicationId(),
	// application.getApplicationAttemptId());
	// }
	// for (Priority priority : application.getPriorities()) {
	// ResourceRequest request = application.getResourceRequest(priority,
	// ResourceRequest.ANY);
	// if (request.getNumContainers() > 0) {
	// if (priority.getPriority() <= 0) {
	// appInfo.setAppMaster(true);
	// appInfo.setReduce(false);
	// appInfo.setAppMasterRequest(request);
	// } else if (priority.getPriority() <= 10) {
	// appInfo.setReduce(true);
	// appInfo.setAppMaster(false);
	// appInfo.setReduceRequest(request);
	// } else if (priority.getPriority() <= 20) {
	// appInfo.setAppMaster(false);
	// appInfo.setReduce(false);
	// appInfo.setMapRequest(request);
	// }
	// }
	// }
	// this.applicationinfos.put(application.getApplicationId(), appInfo);
	// }
	// }

	/**
	 * fill the global variables
	 * 
	 * @param ask
	 *            request of the application
	 */
	private void fillGlobalVars(FiCaSchedulerApp application) {
		// fill the appMaster and containerByResources
		if (application == null)
			return;
		application.showRequests();
		synchronized (application) {
			for (Priority priority : application.getPriorities()) {

			}
		}
		// for (Container container : containers) {
		// if (container.getPriority().getPriority() == 0) {
		// this.appMasters.add(container.getId().toString());
		// } else {
		// synchronized (this.containerByResources) {
		// this.containerByResources.put(container.getId().toString(),
		// container.getResource());
		// }
		// }
		// }
	}

	@Override
	public QueueMetrics getRootQueueMetrics() {
		// TODO Auto-generated method stub
		return this.queue.getMetrics();
	}

	@Override
	public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl, String queueName) {
		// TODO Auto-generated method stub
		return this.queue.hasAccess(acl, callerUGI);
	}

	@Override
	public List<ApplicationAttemptId> getAppsInQueue(String queueName) {
		// TODO Auto-generated method stub
		if (queueName.equals(this.queue.getQueueName())) {
			List<ApplicationAttemptId> attempts = new ArrayList<ApplicationAttemptId>(this.applications.size());
			for (SchedulerApplication<FiCaSchedulerApp> app : this.applications.values()) {
				attempts.add(app.getCurrentAppAttempt().getApplicationAttemptId());
			}
			return attempts;
		} else {
			return null;
		}
	}

	@Override
	public void handle(SchedulerEvent event) {
		// TODO Auto-generated method stub
		switch (event.getType()) {
		case NODE_ADDED: {// 当添加一个新的节点时，触发该事件
			NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent) event;
			addNode(nodeAddedEvent.getAddedRMNode());
			recoverContainersOnNode(nodeAddedEvent.getContainerReports(), nodeAddedEvent.getAddedRMNode());
			break;
		}
		case NODE_REMOVED: {// 当移除一个节点时，触发该事件
			NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent) event;
			removeNode(nodeRemovedEvent.getRemovedRMNode());
			break;
		}
		case NODE_UPDATE: {// 最主要的事件，主要触发NodeManager的心跳机制
			NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent) event;
			nodeUpdate(nodeUpdatedEvent.getRMNode());
			break;
		}
		case APP_ADDED: {// 当添加一个应用程序到ResourceManager，触发该事件
			AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
			addApplication(appAddedEvent.getApplicationId(), appAddedEvent.getQueue(), appAddedEvent.getUser());
			break;
		}
		case APP_REMOVED: {// 当移除一个应用程序时，触发该事件
			AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent) event;
			removeApplication(appRemovedEvent.getApplicationID(), appRemovedEvent.getFinalState());
			break;
		}
		case APP_ATTEMPT_ADDED: {// 当添加一个应用程序的尝试，就触发该事件
			AppAttemptAddedSchedulerEvent appAttemptAddedEvent = (AppAttemptAddedSchedulerEvent) event;
			addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
					appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
					appAttemptAddedEvent.getShouldNotifyAttemptAdded());
			break;
		}
		case APP_ATTEMPT_REMOVED: {// 当一个应用程序的尝试移除时，触发该事件
			AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent = (AppAttemptRemovedSchedulerEvent) event;
			try {
				removeApplicationAttempt(appAttemptRemovedEvent.getApplicationAttemptID(),
						appAttemptRemovedEvent.getFinalAttemptState(),
						appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
			} catch (IOException ie) {
				LOG.info("Unable to remove application " + appAttemptRemovedEvent.getApplicationAttemptID(), ie);
			}
			break;
		}
		case CONTAINER_EXPIRED: {// 当资源调度器将一个Container分配给一个AM是，AM在一定时间间隔内没有使用该Container，就对Container再分配
			ContainerExpiredSchedulerEvent containerExpiredEvent = (ContainerExpiredSchedulerEvent) event;
			ContainerId containerId = containerExpiredEvent.getContainerId();
			containerCompleted(getRMContainer(containerId),
					SchedulerUtils.createAbnormalContainerStatus(containerId, SchedulerUtils.EXPIRED_CONTAINER),
					RMContainerEventType.EXPIRE);
			break;
		}
		default:// 没有上述事件
			LOG.info("Invalid eventType " + event.getType() + ". Ignoring!");
			break;
		}
	}

	private void nodeUpdate(RMNode rmNode) {
		FiCaSchedulerNode node = getNode(rmNode.getNodeID());
		// Update resource if any change
		SchedulerUtils.updateResourceIfChanged(node, rmNode, clusterResource, LOG);

		// Resource nodeResource = node.getAvailableResource();
		// this.allResourceArray.put(node.getNodeID(), nodeResource);

		/**
		 * added by ericson at 2015-07-15
		 */
		// Resource nodeResource = node.getAvailableResource();
		// synchronized (nodeResources) {
		// if (!Resources.equals(nodeResource,
		// nodeResources.get(rmNode.getNodeID()))) {
		// noderesourceChange = true;
		// nodeResources.put(rmNode.getNodeID(), nodeResource);
		// }
		// }

		List<UpdatedContainerInfo> containerInfoList = rmNode.pullContainerUpdates();
		List<ContainerStatus> newlyLaunchedContainers = new ArrayList<ContainerStatus>();
		List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
		for (UpdatedContainerInfo containerInfo : containerInfoList) {
			newlyLaunchedContainers.addAll(containerInfo.getNewlyLaunchedContainers());
			completedContainers.addAll(containerInfo.getCompletedContainers());
		}
		// Processing the newly launched containers
		for (ContainerStatus launchedContainer : newlyLaunchedContainers) {
			containerLaunchedOnNode(launchedContainer.getContainerId(), node);
		}
		// Process completed containers
		for (ContainerStatus completedContainer : completedContainers) {
			ContainerId containerId = completedContainer.getContainerId();
			containerCompleted(getRMContainer(containerId), completedContainer, RMContainerEventType.FINISHED);

			beginRealtime = true;
			this.stopInitialize = false;
		}

		if (Resources.greaterThanOrEqual(resourceCalculator, clusterResource, node.getAvailableResource(),
				this.minimumAllocation)) {
			/**
			 * added by ericson at 2015-07-15
			 */
			if (initialize && stopInitialize) {
				// MergeContainerRequest();
				initialContainerCalculate(node);
				// printJobInfos();
				launchAssignContainers(node);
				// printJobInfos();
			}
			if (this.beginRealtime) {
				// if the assignContainerList is not empty,merge it with the
				// allContainers
				// MergeContainerRequest();
				// get the resource request meet the node resource
				fillContainersByNode(node); // why the resource request be
											// changed
				// printJobInfos();
				// begin to calculate the P
				// Map<Double, Entry<ContainerProfile, Integer>> queue = new
				// ConcurrentSkipListMap<Double, Entry<ContainerProfile,
				// Integer>>(
				// new ContainerComparator());
				PriorityQueue<PContainerProfile> queue = new PriorityQueue<PContainerProfile>(20,
						new ContainerComparator());
				// Map<Double, ContainerProfile> queueP = new
				// ConcurrentSkipListMap<Double, ContainerProfile>(
				// new ContainerComparator());
				calculateContainerP(node, queue);
				// printJobInfos();
				launchAssignContainers(node, queue);
				// printJobInfos();
				// beginRealtime = false;
				// stopInitialize = true;
			}

			// assignContainers(node);
			// assignTasks(node);
			// PriorityQueue<ContainerInfo> containerQueue = new
			// PriorityQueue<ContainerInfo>();
			// initialTaskCalculate(node);
			// List<ContainerInfo> containers = new ArrayList<ContainerInfo>();
			// getContainerToAssign(containers, node);
			// this.containersToAssignWithP(containers, node);
			// launchContainers(node, containers);

		}
		updateAvailableResourcesMetrics();

		/**
		 * added by ericson at 2015-07-15
		 */
		noderesourceChange = false;
		// migration code from MRv1
		// assignTasks(rmNode);
	}

	class PContainerProfile {
		private double P;
		private ContainerProfile containerprofile;

		public PContainerProfile(double p, ContainerProfile profile) {
			P = p;
			containerprofile = profile;
		}

		public double getP() {
			return P;
		}

		public ContainerProfile getContainerprofile() {
			return containerprofile;
		}

	}

	class ContainerComparator implements Comparator<PContainerProfile> {

		@Override
		public int compare(PContainerProfile o1, PContainerProfile o2) {
			// TODO Auto-generated method stub
			double v1 = o1.getP();
			double v2 = o2.getP();
			if (v1 != v2)
				return Double.compare(v2, v1);
			int p1 = o1.getContainerprofile().getContainerRequest().getPriority().getPriority();
			int p2 = o2.getContainerprofile().getContainerRequest().getPriority().getPriority();
			if (p1 == 0)
				return -1;
			if (p2 == 0)
				return 1;
			return Integer.compare(p2, p1);
		}

	}

	private void calculateContainerP(FiCaSchedulerNode node, PriorityQueue<PContainerProfile> queueP) {
		synchronized (assignContainersList) {
			if (!assignContainersList.isEmpty()) {
				double[] f_containers = new double[assignContainersList.size()];
				double[] u_containers = new double[assignContainersList.size()];
				int count = 0;
				for (Map.Entry<ContainerProfile, Integer> entry : assignContainersList.entrySet()) {
					ContainerProfile profile = entry.getKey();
					f_containers[count] = getContainerFitness(profile, node);
					u_containers[count] = getContainerUrgency(profile);
					if (u_containers[count] == Double.MAX_VALUE)
						f_containers[count] = Double.MAX_VALUE;
					count++;
				}
				// double fmin = Double.MAX_VALUE;
				// double fmax = 0;
				// double umin = Double.MAX_VALUE;
				// double umax = 0;
				// for (int i = 0; i < f_containers.length; i++) {
				// if (fmin > f_containers[i])
				// fmin = f_containers[i];
				// if (fmax < f_containers[i])
				// fmax = f_containers[i];
				// if (umin > u_containers[i])
				// umin = u_containers[i];
				// if (umax < u_containers[i])
				// umax = u_containers[i];
				// }
				double[] fu = getMinMax(f_containers, u_containers);
				double flen = fu[1] - fu[0];
				double ulen = fu[3] - fu[2];
				int i = 0;
				// List<ContainerProfile> removeKeyLists = new
				// ArrayList<ContainerProfile>();
				for (Map.Entry<ContainerProfile, Integer> entry : assignContainersList.entrySet()) {
					double ftemp = flen == 0 ? 0 : (f_containers[i] - fu[0]) / flen;
					double utemp = ulen == 0 ? 0 : (u_containers[i] - fu[2]) / ulen;
					double p = ftemp + utemp;
					// queue.put(p, entry);
					PContainerProfile pp = new PContainerProfile(p, entry.getKey());
					queueP.offer(pp);
					// removeKeyLists.add(entry.getKey());
					// assignContainersList.remove(entry.getKey());
					i++;
				}
				// for (ContainerProfile profile : removeKeyLists) {
				// assignContainersList.remove(profile);
				// }
				assignContainersList.clear();
			}
		}
	}

	private double[] getMinMax(double[] f_containers, double[] u_containers) {
		double[] fu = new double[4];
		double fmin = Double.MAX_VALUE;
		double fmax = 0;
		double umin = Double.MAX_VALUE;
		double umax = 0;
		for (int i = 0; i < f_containers.length; i++) {
			if (fmin > f_containers[i])
				fmin = f_containers[i];
			if (fmax < f_containers[i])
				fmax = f_containers[i];
			if (umin > u_containers[i])
				umin = u_containers[i];
			if (umax < u_containers[i])
				umax = u_containers[i];
		}
		fu[0] = fmin;
		fu[1] = fmax;
		fu[2] = umin;
		fu[3] = umax;
		return fu;
	}

	private double getContainerFitness(ContainerProfile profile, FiCaSchedulerNode node) {
		double fitness = 0.0;
		Resource resource = node.getAvailableResource();
		Resource profileResource = profile.getContainerRequest().getCapability();
		double nmem = resource.getMemory() / this.minimumAllocation.getMemory();
		double nvcore = resource.getVirtualCores() / this.minimumAllocation.getVirtualCores();
		double pmem = profileResource.getMemory() / this.minimumAllocation.getMemory();
		double pvcore = profileResource.getVirtualCores() / this.minimumAllocation.getVirtualCores();
		fitness = nmem * pmem * weightResources[0] + nvcore * pvcore * weightResources[1];
		return fitness;
	}

	/**
	 * added by ericson at 2015-07-16
	 * 
	 * @param profile
	 * @return TM: total number of map tasks of job i am: the number of
	 *         ApplicationMaster tasks that have been assigned for job i ar: the
	 *         number of Reduce tasks that have been assigned for job i rr: the
	 *         resource requirement of a single reduce task rrw: the weight of
	 *         the rr resource ram: the resource requirement of a single
	 *         applicationmaster task ramw: the weight of the ram resource aam:
	 *         the number of applicationmaster tasks that have been assigned for
	 *         job i um: the urgency of map task i rm: the resource requiredment
	 *         of a single map task rmw: the weight of the rm resource
	 * 
	 */
	private double getContainerUrgency(ContainerProfile profile) {
		double urgency = 0.0;
		ApplicationId applicationid = profile.getApplicationid();
		JobInfo jobinfo = jobinfos.get(applicationid);
		int TM = jobinfo.getMapNum();
		int am = 0;
		if (jobinfo.getMapProfile() != null)
			am = TM - jobinfo.getMapProfile().getNumberOfContainer();
		if (am == TM)
			return Double.MAX_VALUE;
		int ar = 0;
		Resource rr = Resource.newInstance(0, 0);
		if (jobinfo.getReduceProfile() != null) {
			ar = jobinfo.getReduceNum() - jobinfo.getReduceProfile().getNumberOfContainer();
			Resources.addTo(rr, jobinfo.getReduceProfile().getContainerRequest().getCapability());
		}
		double rrw = rr.getMemory() / this.minimumAllocation.getMemory() * weightResources[0]
				+ rr.getVirtualCores() / this.minimumAllocation.getVirtualCores() * weightResources[1];
		Resource ram = jobinfo.getAppMasterProfile().getContainerRequest().getCapability();
		double ramw = ram.getMemory() / this.minimumAllocation.getMemory() * weightResources[0]
				+ ram.getVirtualCores() / this.minimumAllocation.getVirtualCores() * weightResources[1];
		int aam = jobinfo.getAppMasterNum() - jobinfo.getAppMasterProfile().getNumberOfContainer();
		// calculate the map task urgency
		double um = TM == 0 ? 0 : ((double) am / TM * (ar * rrw + aam * ramw));
		if (profile.getContainerRequest().getPriority().getPriority() == 20)
			return um;
		Resource rm = Resource.newInstance(0, 0);
		if (jobinfo.getMapProfile() != null)
			rm = jobinfo.getMapProfile().getContainerRequest().getCapability();
		double rmw = rm.getMemory() / this.minimumAllocation.getMemory() * weightResources[0]
				+ rm.getVirtualCores() / this.minimumAllocation.getVirtualCores() * weightResources[1];
		// follow can be altered later
		int om = getTotalRunningContainers(applicationid, 20);
		int or = getTotalRunningContainers(applicationid, 10);
		// calculate the reduce task urgency
		double ur = (or == 0 || or == 0 || rrw == 0) ? 0 : um * am / TM * (om * rmw + or * rrw) / (or * rrw);
		if (profile.getContainerRequest().getPriority().getPriority() == 10)
			return ur;
		urgency = um + ur;
		return urgency;
	}

	private int getTotalRunningContainers(ApplicationId applicatinid, int priority) {
		int total = 0;
		Collection<RMContainer> rmcontainers = this.applications.get(applicatinid).getCurrentAppAttempt()
				.getLiveContainers();
		Iterator<RMContainer> iters = rmcontainers.iterator();
		while (iters.hasNext()) {
			RMContainer rm = iters.next();
			Container container = rm.getContainer();
			if (container.getPriority().getPriority() == priority) {
				total++;
			}
		}
		return total;
	}

	private void fillContainersByNode(FiCaSchedulerNode node) {
		synchronized (assignContainersList) {
			Resource nodeResource = node.getAvailableResource();
			for (Map.Entry<ContainerProfile, Integer> entry : allContainers.entrySet()) {
				ContainerProfile profile = entry.getKey();
				Resource profileResource = profile.getContainerRequest().getCapability();
				if (profileResource.getMemory() <= nodeResource.getMemory()
						&& profileResource.getVirtualCores() <= nodeResource.getVirtualCores()) {
					assignContainersList.put(profile, entry.getValue());
					// allContainers.remove(profile);
				}
			}
		}
	}

	private void MergeContainerRequest() {
		synchronized (this) {
			// if (!assignContainersList.isEmpty()) {
			for (Map.Entry<ContainerProfile, Integer> entry : assignContainersList.entrySet()) {
				ContainerProfile profile = entry.getKey();
				if (profile.getContainerRequest().getNumContainers() <= 0) {
					assignContainersList.remove(entry.getKey());
					continue;
				}
				if (allContainers.containsKey(profile)) {
					// profile.setNumberOfContainer(this.allContainers.get(profile)
					// + entry.getValue());
					allContainers.put(profile, allContainers.get(profile) + entry.getValue());
				} else {
					allContainers.put(profile, entry.getValue());
				}
			}
		}
		// }
	}

	private boolean judgeResource(Resource r1, Resource r2) {
		boolean flag = false;
		int mem1 = r1.getMemory();
		int vcore1 = r1.getVirtualCores();
		int mem2 = r2.getMemory();
		int vcore2 = r2.getVirtualCores();
		if (mem1 <= mem2 && vcore1 <= vcore2)
			flag = true;
		return flag;
	}

	// private void containersToAssignWithP(List<ContainerInfo> lists,
	// FiCaSchedulerNode node) {
	// if (!this.applicationinfos.isEmpty()) {
	// Resource nodeResource = node.getAvailableResource();
	// Resource zero = Resource.newInstance(0, 0);
	// synchronized (this.applicationinfos) {
	// while (judgeResource(zero, nodeResource) && this.applicationinfos.size()
	// > 0) {
	// double[] containerP = new double[this.applicationinfos.size()];
	// double[][] fu = this.getAllFU(node);
	// double maxF = 0;
	// double minF = Integer.MAX_VALUE;
	// double maxU = 0;
	// double minU = Integer.MAX_VALUE;
	// for (int i = 0; i < fu.length; i++) {
	// maxF = Math.max(maxF, fu[i][0]);
	// minF = Math.min(minF, fu[i][0]);
	// maxU = Math.max(maxU, fu[i][1]);
	// minU = Math.min(minU, fu[i][1]);
	// }
	// for (int i = 0; i < this.applicationinfos.size(); i++) {
	// containerP[i] = (fu[i][0] - minF) / (maxF - minF) + (fu[i][1] - minU) /
	// (maxU - minU);
	// }
	// double maxfitness = containerP[0];
	// int index = 0;
	// for (int i = 1; i < containerP.length; i++) {
	// if (maxfitness < containerP[i]) {
	// maxfitness = containerP[i];
	// index = i;
	// }
	// }
	// ApplicationInfo requestadd = this.applicationinfos.get(index);
	// ApplicationId applicationid = requestadd.getAppId();
	// ContainerInfo containerinfo = null;
	// int num = 0;
	// if (requestadd.isAppMaster()) {
	// containerinfo = new ContainerInfo(applicationid,
	// requestadd.getCurrentAttemptId(),
	// this.applications.get(applicationid).getCurrentAppAttempt().getNewContainerId(),
	// requestadd.getAppMasterRequest());
	// Resources.addTo(zero, requestadd.getAppMasterRequest().getCapability());
	// num = requestadd.getAppMasterRequest().getNumContainers();
	// } else if (requestadd.isReduce()) {
	// containerinfo = new ContainerInfo(applicationid,
	// requestadd.getCurrentAttemptId(),
	// this.applications.get(applicationid).getCurrentAppAttempt().getNewContainerId(),
	// requestadd.getReduceRequest());
	// Resources.addTo(zero, requestadd.getReduceRequest().getCapability());
	// num = requestadd.getReduceRequest().getNumContainers();
	// } else {
	// containerinfo = new ContainerInfo(applicationid,
	// requestadd.getCurrentAttemptId(),
	// this.applications.get(applicationid).getCurrentAppAttempt().getNewContainerId(),
	// requestadd.getMapRequest());
	// Resources.addTo(zero, requestadd.getMapRequest().getCapability());
	// num = requestadd.getMapRequest().getNumContainers();
	// }
	// lists.add(containerinfo);
	// if (num - 1 == 0) {
	// this.applicationinfos.remove(index);
	// } else {
	// this.applicationinfos.get(index).getAppMasterRequest().setNumContainers(num
	// - 1);
	// }
	// }
	// }
	// }
	// }

	// private void getContainerToAssign(List<ContainerInfo> lists,
	// FiCaSchedulerNode node) {
	// if (!this.applicationInfoQueue.isEmpty()) {
	// Resource nodeResource = node.getAvailableResource();
	// Resource zero = Resource.newInstance(0, 0);
	// synchronized (this.applicationInfoQueue) {
	// while (judgeResource(zero, nodeResource) &&
	// this.applicationInfoQueue.size() > 0) {
	// double[] containerfitness = new double[this.applicationInfoQueue.size()];
	// for (int i = 0; i < this.applicationInfoQueue.size(); i++) {
	// ApplicationRequestInfo applicationinfo =
	// this.applicationInfoQueue.get(i);
	// containerfitness[i] =
	// this.getContainerFitness(applicationinfo.getRequest(), node);
	// }
	// double maxfitness = containerfitness[0];
	// int index = 0;
	// for (int i = 1; i < containerfitness.length; i++) {
	// if (maxfitness < containerfitness[i]) {
	// maxfitness = containerfitness[i];
	// index = i;
	// }
	// }
	// ApplicationRequestInfo requestadd = this.applicationInfoQueue.get(index);
	// ApplicationId applicationid = requestadd.getApplicationId();
	// ContainerInfo containerinfo = new ContainerInfo(applicationid,
	// requestadd.getAppAttemptId(),
	// this.applications.get(applicationid).getCurrentAppAttempt().getNewContainerId(),
	// requestadd.getRequest());
	// lists.add(containerinfo);
	// Resources.addTo(zero, requestadd.getRequest().getCapability());
	// int num = requestadd.getRequest().getNumContainers();
	// if (num - 1 == 0) {
	// this.applicationInfoQueue.remove(index);
	// } else {
	// this.applicationInfoQueue.get(index).getRequest().setNumContainers(num -
	// 1);
	// }
	// }
	// }
	// }
	// }

	// private void getContainerInfos(PriorityQueue<ContainerInfo> queue) {
	// if (this.applicationInfoQueue.isEmpty())
	// return;
	//
	// }

	private void launchAssignContainers(FiCaSchedulerNode node) {
		if (Resources.lessThan(resourceCalculator, clusterResource, node.getAvailableResource(),
				this.minimumAllocation))
			return;
		synchronized (allContainers) {
			for (Map.Entry<ContainerProfile, Integer> entry : assignContainersList.entrySet()) {
				ContainerProfile profile = entry.getKey();
				// for (int i = 0; i < entry.getValue(); ) {
				if (profile.getContainerRequest().getPriority().getPriority() == 10
						&& this.getTotalRunningContainers(profile.getApplicationid(), 20) > 0) {
					continue;
				}
				int result = assignContainersOnNode(node, profile, entry.getValue());
				if (result != -1) {
					// profile.setRequestNumber(profile.getContainerRequest().getNumContainers()
					// - result);
					/*
					 * if (entry.getValue() - result <= 0) {
					 * assignContainersList.remove(entry.getKey()); } else {
					 * assignContainersList.put(profile, entry.getValue() -
					 * result); }
					 */
					if (allContainers.containsKey(profile)) {
						int num = allContainers.get(profile);
						if (num <= result) {
							allContainers.remove(profile);
							profile.setNumberOfContainer(0);
						} else {
							allContainers.put(profile, num - result);
							profile.setNumberOfContainer(num - result);
						}
					}
				}
				// }
			}
			assignContainersList.clear();
		}
	}

	private void launchAssignContainers(FiCaSchedulerNode node, PriorityQueue<PContainerProfile> queueP) {
		if (Resources.lessThan(resourceCalculator, clusterResource, node.getAvailableResource(),
				this.minimumAllocation))
			return;
		synchronized (allContainers) {
			while (!queueP.isEmpty()) {
				ContainerProfile profile = queueP.peek().getContainerprofile();
				if (profile.getContainerRequest().getPriority().getPriority() == 10
						&& getTotalRunningContainers(profile.getApplicationid(), 20) > 0) {
					queueP.poll();
					continue;
				}
				int willAssignContainers = getAvailableContainers(node, profile);
				int result = assignContainersOnNode(node, profile, willAssignContainers);
				if (result != -1) {
					int num = allContainers.get(profile);
					if (num <= result) {
						allContainers.remove(profile);
						profile.setNumberOfContainer(0);
					} else {
						allContainers.put(profile, num - result);
						profile.setNumberOfContainer(num - result);
					}
				}
				queueP.poll();
			}
			// for (Map.Entry<Double, Entry<ContainerProfile, Integer>> entry :
			// queue.entrySet()) {
			// ContainerProfile profile = entry.getValue().getKey();
			// int willAssignContainers = entry.getValue().getValue();
			// // for (int i = 0; i < entry.getValue(); ) {
			// int result = assignContainersOnNode(node, profile,
			// willAssignContainers);
			// if (result != -1) {
			// // profile.setNumberOfContainer(profile.getNumberOfContainer()
			// // - result);
			// // profile.setRequestNumber(willAssignContainers - result);
			// if (willAssignContainers - result > 0) {
			// assignContainersList.put(profile, willAssignContainers - result);
			// }
			// }
			// // }
			// }
		}
	}

	private int getAvailableContainers(FiCaSchedulerNode node, ContainerProfile profile) {
		int assign = 0;
		Resource nodeResource = node.getAvailableResource();
		Resource profileResource = profile.getContainerRequest().getCapability();
		// int num = profile.getContainerRequest().getNumContainers();
		if (profileResource.getMemory() <= nodeResource.getMemory()
				&& profileResource.getVirtualCores() <= nodeResource.getVirtualCores()) {
			assign = Math.min(nodeResource.getMemory() / profileResource.getMemory(),
					nodeResource.getVirtualCores() / profileResource.getVirtualCores());
		}
		return assign;
	}

	// private void launchContainers(FiCaSchedulerNode node, List<ContainerInfo>
	// containers) {
	// if (Resources.lessThan(resourceCalculator, clusterResource,
	// node.getAvailableResource(),
	// this.minimumAllocation))
	// return;
	// // synchronized (this.containerInfoQueue) {
	// // while (!this.containerInfoQueue.isEmpty()) {
	// // ContainerInfo peekContainerinfo = this.containerInfoQueue.peek();
	// // SchedulerApplication<FiCaSchedulerApp> app = this.applications
	// // .get(peekContainerinfo.getApplicationId());
	// // FiCaSchedulerApp application = app.getCurrentAppAttempt();
	// // NodeId nodeId = node.getRMNode().getNodeID();
	// // ContainerId containerId =
	// // BuilderUtils.newContainerId(application.getApplicationAttemptId(),
	// // peekContainerinfo.getContainerId());
	// // Container container = BuilderUtils.newContainer(containerId, nodeId,
	// // node.getHttpAddress(),
	// // peekContainerinfo.getRequest().getCapability(),
	// // peekContainerinfo.getRequest().getPriority(),
	// // null);
	// // RMContainer rmContainer = application.allocate(NodeType.OFF_SWITCH,
	// // node,
	// // peekContainerinfo.getRequest().getPriority(),
	// // peekContainerinfo.getRequest(), container);
	// // node.allocateContainer(rmContainer);
	// // this.increaseUsedResources(rmContainer);
	// // this.containerInfoQueue.poll();
	// // }
	// // }
	// synchronized (this.applicationInfoQueue) {
	// for (int i = 0; i < containers.size(); i++) {
	// ContainerInfo containerinfo = containers.get(i);
	// SchedulerApplication<FiCaSchedulerApp> app =
	// this.applications.get(containerinfo.getApplicationId());
	// FiCaSchedulerApp application = app.getCurrentAppAttempt();
	// // int countContainers = this.assignContainersOnNode(node,
	// // containerinfo);
	// // NodeId nodeId = node.getRMNode().getNodeID();
	// // ContainerId containerId =
	// // BuilderUtils.newContainerId(application.getApplicationAttemptId(),
	// // containerinfo.getContainerId());
	// // LOG.info("------------AS:containerId:" +
	// // containerId.toString());
	// // Container container = BuilderUtils.newContainer(containerId,
	// // nodeId, node.getHttpAddress(),
	// // containerinfo.getRequest().getCapability(),
	// // containerinfo.getRequest().getPriority(), null);
	// // RMContainer rmContainer =
	// // application.allocate(NodeType.OFF_SWITCH, node,
	// // containerinfo.getRequest().getPriority(),
	// // containerinfo.getRequest(), container);
	// // LOG.info("-----------AS:rmcontainer");
	// // node.allocateContainer(rmContainer);
	// // this.increaseUsedResources(rmContainer);
	// }
	// containers.clear();
	// }
	//
	// // synchronized (this.willAssignContainers) {
	// // for (Map.Entry<String, List<ContainerInfo>> entry :
	// // this.willAssignContainers.entrySet()) {
	// // List<ContainerInfo> containers = entry.getValue();
	// // for (ContainerInfo info : containers) {
	// // SchedulerApplication<FiCaSchedulerApp> app =
	// // this.applications.get(info.getApplicationId());
	// // FiCaSchedulerApp application = app.getCurrentAppAttempt();
	// // NodeId nodeId = node.getRMNode().getNodeID();
	// // ContainerId containerId =
	// // BuilderUtils.newContainerId(application.getApplicationAttemptId(),
	// // info.getContainerId());
	// // Container container = BuilderUtils.newContainer(containerId, nodeId,
	// // node.getHttpAddress(),
	// // info.getRequest().getCapability(), info.getRequest().getPriority(),
	// // null);
	// // RMContainer rmContainer = application.allocate(NodeType.OFF_SWITCH,
	// // node,
	// // info.getRequest().getPriority(), info.getRequest(), container);
	// // node.allocateContainer(rmContainer);
	// // this.increaseUsedResources(rmContainer);
	// // }
	// // }
	// // this.willAssignContainers.clear();
	// // }
	// }

	private int assignContainersOnNode(FiCaSchedulerNode node, ContainerProfile containerprofile,
			int willAssignContainers) {
		if (Resources.lessThan(resourceCalculator, clusterResource, node.getAvailableResource(),
				containerprofile.getContainerRequest().getCapability()))
			return -1;
		// data-local
		int nodeLocalContainers = assignNodeLocalContainers(node, containerprofile, willAssignContainers);
		willAssignContainers -= nodeLocalContainers;
		// rack-local
		int rackLocalContainers = assignRackLocalContainers(node, containerprofile, willAssignContainers);
		willAssignContainers -= rackLocalContainers;
		// off-switch
		int OffSwitchContainers = assignOffSwitchContainers(node, containerprofile, willAssignContainers);
		return nodeLocalContainers + rackLocalContainers + OffSwitchContainers;
	}

	private int assignOffSwitchContainers(FiCaSchedulerNode node, ContainerProfile containerprofile,
			int willAssignContainers) {
		int assignedContainers = 0;
		FiCaSchedulerApp application = this.applications.get(containerprofile.getApplicationid())
				.getCurrentAppAttempt();
		Priority priority = containerprofile.getContainerRequest().getPriority();
		ResourceRequest request = application.getResourceRequest(priority, ResourceRequest.ANY);
		if (request != null) {
			if (request.getNumContainers() <= 0)
				return 0;
			int assignableContainers = Math.min(request.getNumContainers(), willAssignContainers);
			// containerprofile.setContainerRequest(request);
			// assignedContainers = assignContainer(node, containerprofile,
			// NodeType.OFF_SWITCH, willAssignContainers);
			assignedContainers = assignContainer(node, application, priority, assignableContainers, request,
					NodeType.OFF_SWITCH);
		}
		return assignedContainers;
	}

	private int assignRackLocalContainers(FiCaSchedulerNode node, ContainerProfile containerprofile,
			int willAssignContainers) {
		int assignedContainers = 0;
		FiCaSchedulerApp application = this.applications.get(containerprofile.getApplicationid())
				.getCurrentAppAttempt();
		Priority priority = containerprofile.getContainerRequest().getPriority();
		ResourceRequest request = application.getResourceRequest(priority, node.getRMNode().getRackName());
		if (request != null) {
			ResourceRequest offSwitchRequest = application.getResourceRequest(priority, ResourceRequest.ANY);
			if (offSwitchRequest.getNumContainers() <= 0)
				return 0;
			int assignableContainers = Math.min(willAssignContainers,
					Math.min(getMaxAllocatableContainers(application, priority, node, NodeType.RACK_LOCAL),
							request.getNumContainers()));
			assignedContainers = assignContainer(node, application, priority, assignableContainers, request,
					NodeType.RACK_LOCAL);
		}
		return assignedContainers;
	}

	private int assignNodeLocalContainers(FiCaSchedulerNode node, ContainerProfile containerprofile,
			int willAssignContainers) {
		int assignedContainers = 0;
		FiCaSchedulerApp application = this.applications.get(containerprofile.getApplicationid())
				.getCurrentAppAttempt();
		Priority priority = containerprofile.getContainerRequest().getPriority();
		ResourceRequest request = application.getResourceRequest(priority, node.getNodeName());
		if (request != null) {
			ResourceRequest rackRequest = application.getResourceRequest(priority, node.getRMNode().getRackName());
			if (rackRequest == null || rackRequest.getNumContainers() <= 0) {
				return 0;
			}
			int assignableContainers = Math.min(willAssignContainers,
					Math.min(getMaxAllocatableContainers(application, priority, node, NodeType.NODE_LOCAL),
							request.getNumContainers()));
			assignedContainers = assignContainer(node, application, priority, assignableContainers, request,
					NodeType.NODE_LOCAL);
		}
		return assignedContainers;
	}

	private int assignContainer(FiCaSchedulerNode node, ContainerProfile containerprofile, NodeType type,
			int willAssignContainers) {
		ResourceRequest request = containerprofile.getContainerRequest();
		int assignableContainers = Math.min(willAssignContainers, request.getNumContainers());
		FiCaSchedulerApp application = this.applications.get(containerprofile.getApplicationid())
				.getCurrentAppAttempt();
		Priority priority = request.getPriority();
		Resource capability = request.getCapability();
		int availableContainers = node.getAvailableResource().getMemory() / capability.getMemory();
		int assignedContainers = Math.min(assignableContainers, availableContainers);
		if (assignableContainers > 0) {
			for (int i = 0; i < assignedContainers; ++i) {
				NodeId nodeId = node.getRMNode().getNodeID();
				ContainerId containerId = BuilderUtils.newContainerId(application.getApplicationAttemptId(),
						application.getNewContainerId());
				Container container = BuilderUtils.newContainer(containerId, nodeId, node.getRMNode().getHttpAddress(),
						capability, priority, null);
				RMContainer rmContainer = application.allocate(type, node, priority, request, container);
				node.allocateContainer(rmContainer);
				increaseUsedResources(rmContainer);
			}
		}
		return assignedContainers;
	}

	// private void assignTasks() {
	// for (Map.Entry<ApplicationId, SchedulerApplication<FiCaSchedulerApp>> e :
	// this.applications.entrySet()) {
	// FiCaSchedulerApp application = e.getValue().getCurrentAppAttempt();
	// if (application == null)
	// continue;
	// synchronized (application) {
	// // if (SchedulerAppUtils.isBlacklisted(application, node, LOG))
	// // continue;
	// for (Priority priority : application.getPriorities()) {
	// // Map<String, ResourceRequest> maps =
	// // application.getResourceRequests(priority);
	// // test the resource request
	// ResourceRequest request = application.getResourceRequest(priority,
	// ResourceRequest.ANY);
	// // for (Map.Entry<String, ResourceRequest> entry :
	// // maps.entrySet()) {
	// // ResourceRequest request = entry.getValue();
	// int num = request.getNumContainers();
	// if (num > 0) {
	// synchronized (this.applicationInfoQueue) {
	// ApplicationRequestInfo applicationRequest = new ApplicationRequestInfo(
	// application.getApplicationId(), application.getApplicationAttemptId(),
	// request);
	// this.applicationInfoQueue.add(applicationRequest);
	// }
	// }
	// // for (int i = 0; i < num; i++) {
	// // int containerId = application.getNewContainerId();
	// // ContainerInfo containerinfo = new
	// // ContainerInfo(application.getApplicationId(),
	// // application.getApplicationAttemptId(), containerId,
	// // request);
	// // synchronized (this.containerInfoQueue) {
	// // this.containerInfoQueue.offer(containerinfo);
	// // }
	// // }
	// // }
	// }
	// }
	// }
	// }

	// private double[][] getAllFU(FiCaSchedulerNode node) {
	// double[][] fu = new double[this.applicationinfos.size()][2];
	// for (int i = 0; i < this.applicationinfos.size(); i++) {
	// ApplicationInfo appinfo = this.applicationinfos.get(i);
	// double ui = this.getMapContainerUrgency(appinfo.getAppId())
	// + this.getReduceContainerUrgency(appinfo.getAppId());
	// double fi = this.getContainerFitness(appinfo.getMapRequest(), node);
	// fu[i][0] = ui;
	// fu[i][1] = fi;
	// }
	// return fu;
	// }

	// private double getContainerFitness(ResourceRequest request,
	// FiCaSchedulerNode node) {
	// double fitness = 0.0;
	// Resource containerResource = request.getCapability();
	// int taskMem = containerResource.getMemory() /
	// this.minimumAllocation.getMemory();
	// int taskVcore = containerResource.getVirtualCores() /
	// this.minimumAllocation.getVirtualCores();
	// Resource nodeResource = this.allResourceArray.get(node.getNodeID());
	// int nodeMem = nodeResource.getMemory() /
	// this.minimumAllocation.getMemory();
	// int nodeVcore = nodeResource.getVirtualCores() /
	// this.minimumAllocation.getVirtualCores();
	// fitness = taskMem * nodeMem * this.resourceWeights[0] + taskVcore *
	// nodeVcore * this.resourceWeights[1];
	// return fitness;
	// }

	// private double getMapContainerUrgency(ApplicationId appId) {
	// double mapUrgency = 0.0;
	// ApplicationInfo appinfo = this.applicationinfos.get(appId);
	// ResourceRequest appMasterRequest = appinfo.getAppMasterRequest();
	// ResourceRequest reduceRequest = appinfo.getReduceRequest();
	// double ri = reduceRequest.getCapability().getMemory() /
	// this.minimumAllocation.getMemory()
	// * this.resourceWeights[0]
	// + reduceRequest.getCapability().getVirtualCores() /
	// this.minimumAllocation.getVirtualCores()
	// * this.resourceWeights[1];
	// double ram = appMasterRequest.getCapability().getMemory() /
	// this.minimumAllocation.getMemory()
	// * this.resourceWeights[0]
	// + appMasterRequest.getCapability().getVirtualCores() /
	// this.minimumAllocation.getVirtualCores()
	// * this.resourceWeights[1];
	// mapUrgency = (appinfo.getTotalMapContainers() -
	// appinfo.getMapContainers()) / appinfo.getTotalMapContainers()
	// * ((appinfo.getTotalReduceContainers() - appinfo.getReduceContainers()) *
	// ri
	// + appinfo.getTotalAppMasterContainers() * ram);
	// return mapUrgency;
	// }

	// private double getReduceContainerUrgency(ApplicationId appId) {
	// double reduceUrgency = 0.0;
	// ApplicationInfo appinfo = this.applicationinfos.get(appId);
	// double um = this.getMapContainerUrgency(appId);
	// reduceUrgency = um * (appinfo.getTotalMapContainers() -
	// appinfo.getMapContainers())
	// / appinfo.getTotalMapContainers();
	// Collection<RMContainer> collections =
	// this.applications.get(appId).getCurrentAppAttempt().getLiveContainers();
	// Iterator<RMContainer> ites = collections.iterator();
	// int om = 0;
	// int or = 0;
	// while (ites.hasNext()) {
	// RMContainer container = ites.next();
	// if (container.getAllocatedPriority().getPriority() <= 10
	// && container.getAllocatedPriority().getPriority() > 0) {
	// or++;
	// } else if (container.getAllocatedPriority().getPriority() <= 20)
	// om++;
	// }
	// ResourceRequest mapRequest = appinfo.getMapRequest();
	// ResourceRequest reduceRequest = appinfo.getReduceRequest();
	// double rm = mapRequest.getCapability().getMemory() /
	// this.minimumAllocation.getMemory()
	// * this.resourceWeights[0]
	// + mapRequest.getCapability().getVirtualCores() /
	// this.minimumAllocation.getVirtualCores()
	// * this.resourceWeights[1];
	// double rr = reduceRequest.getCapability().getMemory() /
	// this.minimumAllocation.getMemory()
	// * this.resourceWeights[0]
	// + reduceRequest.getCapability().getVirtualCores() /
	// this.minimumAllocation.getVirtualCores()
	// * this.resourceWeights[1];
	// reduceUrgency *= (om * rm + or * rr) / (or * rr);
	// return reduceUrgency;
	// }

	// private void assignTasks(RMNode rmNode) {
	// TrackerInfo tinfo = cluster.updateTracker(rmNode);
	// Assignment desired = tinfo.getAssignment();
	// Map<String, Integer> count = cluster.getCount();
	// Assignment current = new Assignment();
	// FiCaSchedulerNode node = this.nodes.get(rmNode.getNodeID());
	// ApplicationId applicationId = null;
	// for (RMContainer container : node.getRunningContainers()) {
	// applicationId =
	// this.getCurrentAttemptForContainer(container.getContainerId()).getApplicationId();
	// RMContainerState state = container.getState();
	// if (state == RMContainerState.EXPIRED || state == RMContainerState.KILLED
	// || state == RMContainerState.RELEASED) {
	// current.put(applicationId.toString());
	// }
	// }
	//
	// float cpu = 0.0f;
	// float io = 0.0f;
	// float mem = 0.0f;
	//
	// for (Entry<String, Integer> entry : current.getContainers()) {
	// String jid = entry.getKey();
	// Integer num = entry.getValue();
	// JobInfo jinfo = jobsByID.get(jid);
	// if (jinfo == null)
	// continue;
	// JobProfile profile = jinfo.getProfile();
	// cpu += profile.getCpuUsage() * num;
	// io += profile.getIoUsage() * num;
	// mem += profile.getMemUsage() * num;
	// }
	//
	// Assignment include = current.toInclude(desired);
	// ArrayList<RMContainer> tasks = new ArrayList<RMContainer>();
	// for (String jid : include.getJobs()) {
	// JobInfo jinfo = jobsByID.get(jid);
	// if (jinfo == null)
	// continue;
	// JobProfile profile = jinfo.getProfile();
	// int num = include.getNumContainers();
	// for (int i = 0; i < num; i++) {
	// if ((cpu + profile.getCpuUsage() > UTILIZATION) || (io +
	// profile.getIoUsage() > UTILIZATION)
	// || (mem + profile.getMemUsage() > UTILIZATION))
	// break;
	//
	// }
	// }
	// }

	// private RMContainer getContainer(RMNode taskTracker, String jid) {
	// int numHosts = this.nodes.size();
	// JobInfo jinfo = jobsByID.get(jid);
	// if (jinfo == null)
	// return null;
	// FiCaSchedulerApp job = jinfo.getJobInProgress();
	// if (job == null)
	// return null;
	// RMContainer task = null;
	// // get RMContainer
	// return task;
	// }

	private void updateAvailableResourcesMetrics() {
		this.queue.getMetrics().setAvailableResourcesToQueue(Resources.subtract(clusterResource, usedResources));
	}

	/**
	 * Heart of the scheduler
	 * 
	 * @param node
	 *            , on which resources are available to be allocated
	 */
	private void assignContainers(FiCaSchedulerNode node) {
		for (Map.Entry<ApplicationId, SchedulerApplication<FiCaSchedulerApp>> e : this.applications.entrySet()) {
			FiCaSchedulerApp application = e.getValue().getCurrentAppAttempt();
			if (application == null) {
				continue;
			}
			application.showRequests();
			synchronized (application) {
				if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {
					continue;
				}
				for (Priority priority : application.getPriorities()) {
					int maxContainers = getMaxAllocatableContainers(application, priority, node, NodeType.OFF_SWITCH);
					// Ensure the application needs containers of this priority
					if (maxContainers > 0) {
						int assignedContainers = assignContainersOnNode(node, application, priority);
						if (assignedContainers == 0)
							break;
					}
				}
			}
			application.showRequests();
			if (Resources.lessThan(resourceCalculator, clusterResource, node.getAvailableResource(),
					minimumAllocation)) {
				break;
			}
		}
		for (SchedulerApplication<FiCaSchedulerApp> application : applications.values()) {
			FiCaSchedulerApp attempt = (FiCaSchedulerApp) application.getCurrentAppAttempt();
			if (attempt == null) {
				continue;
			}
			updateAppHeadRoom(attempt);
		}
	}

	// private void calculateContainersOnEachNode() {
	// for (Map.Entry<NodeId, Resource> entry :
	// this.allResourceArray.entrySet()) {
	// NodeId key = entry.getKey();
	// this.initialContainerCalculate(this.nodes.get(key));
	// }
	// }

	private void initialContainerCalculate(FiCaSchedulerNode node) {
		if (this.allContainers.size() == 0)
			return;
		Resource nodeResource = node.getAvailableResource();
		int minMemory = this.minimumAllocation.getMemory();
		int minVCore = this.minimumAllocation.getVirtualCores();
		int m = nodeResource.getMemory() / minMemory;
		int n = nodeResource.getVirtualCores() / minVCore;
		if (m <= 0 || n <= 0)
			return;
		// represent the maximum value of the objective function
		double[][] M = new double[m + 1][n + 1];
		// represent the list of tasks that yield the optimal solution
		Map<Integer, Map<ContainerProfile, Integer>> maps = new HashMap<Integer, Map<ContainerProfile, Integer>>();
		int[][] L = new int[m + 1][n + 1];
		MEM: for (int i = 1; i <= m; i++) {
			VCORE: for (int j = 1; j <= n; j++) {
				Map<ContainerProfile, Integer> tempL = null;
				Map<ContainerProfile, Integer> tl = null;
				// Iterator<ContainerProfile> iters =
				// this.allContainerQueue.iterator();
				for (Map.Entry<ContainerProfile, Integer> entry : this.allContainers.entrySet()) {
					ContainerProfile cinfo = entry.getKey();
					Resource conResource = cinfo.getContainerRequest().getCapability();
					int mem = conResource.getMemory() / this.minimumAllocation.getMemory();
					int vcore = conResource.getVirtualCores() / this.minimumAllocation.getVirtualCores();
					int num = cinfo.getNumberOfContainer();
					if (i >= mem && j >= vcore) {
						for (int t = 1; t <= num; t++) {
							tl = maps.get(L[i - mem][j - vcore]);
							if (tl != null && tl.containsKey(cinfo) && tl.get(cinfo) >= num)
								break;
							Resource totalTl = getResourceTotal(tl);
							// Resources.addTo(totalTl,
							// getResourceTotal(tempL));
							if (totalTl.getMemory() + conResource.getMemory() > i * minMemory)
								break;
							if (totalTl.getVirtualCores() + conResource.getVirtualCores() > j * minVCore)
								break;
							// if (!this.compareListResources(tl, i, j))
							// continue;
							// the memory weight is equal the vcore weight, 0.5
							double v = weightResources[0] * mem + weightResources[1] * vcore;
							double temp = M[i - mem][j - vcore] + v;
							if (M[i][j] < temp) {
								tempL = new HashMap<ContainerProfile, Integer>();
								M[i][j] = temp;
								if (tl != null) {
									for (Map.Entry<ContainerProfile, Integer> e : tl.entrySet()) {
										tempL.put(e.getKey(), e.getValue());
									}
								}
								if (!tempL.containsKey(cinfo)) {
									tempL.put(cinfo, 1);
								} else if (tempL.get(cinfo) < cinfo.getNumberOfContainer())
									tempL.put(cinfo, tempL.get(cinfo) + 1);
							}
						}
					} else if (i < mem) {
						continue MEM;
					} else
						continue VCORE;

				}
				L[i][j] = n * i + j;
				if (tempL == null || tempL.size() > 0)
					maps.put(L[i][j], tempL);
				else
					maps.put(L[i][j], tl);
			}
		}

		// get the maximum M
		// int aindex = 0;
		// int bindex = 0;
		// double max = 0;
		// for (int i = 0; i <= m; i++) {
		// for (int j = 0; j <= n; j++) {
		// if (M[i][j] >= max) {
		// max = M[i][j];
		// aindex = i;
		// bindex = j;
		// }
		// }
		// }
		Map<ContainerProfile, Integer> assigningContainers = maps.get(L[m][n]);
		if (assigningContainers != null) {
			synchronized (assignContainersList) {
				for (Map.Entry<ContainerProfile, Integer> assign : assigningContainers.entrySet()) {
					// if
					// (!this.assignContainersList.containsKey(assign.getKey()))
					// {
					// this.assignContainersList.put(assign.getKey(), );
					// } else {
					// this.assignContainersList.put(assign.getKey(), 1 +
					// this.assignContainersList.get(assign.getKey()));
					// }
					ContainerProfile conprofile = assign.getKey();
					assignContainersList.put(conprofile, assign.getValue());
					/*
					 * if (allContainers.containsKey(conprofile)) { int num =
					 * allContainers.get(conprofile);
					 * allContainers.remove(conprofile); if (assign.getValue() <
					 * num) { // conprofile.setNumberOfContainer(num - //
					 * assign.getValue()); //
					 * conprofile.getContainerRequest().setNumContainers(num //
					 * - assign.getValue()); this.allContainers.put(conprofile,
					 * num - assign.getValue()); } }
					 */
				}
			}
		}

	}

	private Resource getResourceTotal(Map<ContainerProfile, Integer> list) {
		Resource total = Resource.newInstance(0, 0);
		if (list == null)
			return total;
		for (Map.Entry<ContainerProfile, Integer> container : list.entrySet()) {
			ContainerProfile containerprofile = container.getKey();
			int num = container.getValue();
			for (int i = 0; i < num; i++) {
				Resources.addTo(total, containerprofile.getContainerRequest().getCapability());
			}
		}
		return total;
	}

	private boolean compareListResources(List<ContainerInfo> list, int i, int j) {
		Resource nodeResource = Resource.newInstance(i * this.minimumAllocation.getMemory(),
				j * this.minimumAllocation.getVirtualCores());
		Resource conResource = Resource.newInstance(0, 0);
		for (ContainerInfo containerId : list) {
			// Resources.addTo(conResource,
			// this.allResourceArray.get(containerId));
		}
		if (Resources.lessThanOrEqual(resourceCalculator, clusterResource, conResource, nodeResource)) {
			return true;
		}
		return false;
	}

	private void updateAppHeadRoom(SchedulerApplicationAttempt schedulerAttempt) {
		schedulerAttempt.setHeadroom(Resources.subtract(clusterResource, usedResources));
	}

	private int assignContainersOnNode(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority) {
		// data-local
		int nodeLocalContainers = assignNodeLocalContainers(node, application, priority);
		// rack-local
		int rackLocalContainers = assignRackLocalContainers(node, application, priority);
		// off-switch
		int OffSwitchContainers = assignOffSwitchContainers(node, application, priority);
		return nodeLocalContainers + rackLocalContainers + OffSwitchContainers;
	}

	private int assignOffSwitchContainers(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority) {
		int assignedContainers = 0;
		ResourceRequest request = application.getResourceRequest(priority, ResourceRequest.ANY);
		if (request != null) {
			assignedContainers = assignContainer(node, application, priority, request.getNumContainers(), request,
					NodeType.OFF_SWITCH);
		}
		return assignedContainers;
	}

	private int assignRackLocalContainers(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority) {
		int assignedContainers = 0;
		ResourceRequest request = application.getResourceRequest(priority, node.getRMNode().getRackName());
		if (request != null) {
			ResourceRequest offSwitchRequest = application.getResourceRequest(priority, ResourceRequest.ANY);
			if (offSwitchRequest.getNumContainers() <= 0)
				return 0;
			int assignableContainers = Math.min(
					getMaxAllocatableContainers(application, priority, node, NodeType.RACK_LOCAL),
					request.getNumContainers());
			assignedContainers = assignContainer(node, application, priority, assignableContainers, request,
					NodeType.RACK_LOCAL);
		}
		return assignedContainers;
	}

	private int assignNodeLocalContainers(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority) {
		int assignedContainers = 0;
		ResourceRequest request = application.getResourceRequest(priority, node.getNodeName());
		if (request != null) {
			ResourceRequest rackRequest = application.getResourceRequest(priority, node.getRMNode().getRackName());
			if (rackRequest == null || rackRequest.getNumContainers() <= 0) {
				return 0;
			}
			int assignableContainers = Math.min(
					getMaxAllocatableContainers(application, priority, node, NodeType.NODE_LOCAL),
					request.getNumContainers());
			assignedContainers = assignContainer(node, application, priority, assignableContainers, request,
					NodeType.NODE_LOCAL);
		}
		return assignedContainers;
	}

	private int assignContainer(FiCaSchedulerNode node, FiCaSchedulerApp application, Priority priority,
			int assignableContainers, ResourceRequest request, NodeType type) {
		Resource capability = request.getCapability();
		int availableContainers = node.getAvailableResource().getMemory() / capability.getMemory();
		int assignedContainers = Math.min(assignableContainers, availableContainers);
		// if (assignedContainers > 0) {
		for (int i = 0; i < assignedContainers; ++i) {
			NodeId nodeId = node.getRMNode().getNodeID();
			ContainerId containerId = BuilderUtils.newContainerId(application.getApplicationAttemptId(),
					application.getNewContainerId());
			Container container = BuilderUtils.newContainer(containerId, nodeId, node.getRMNode().getHttpAddress(),
					capability, priority, null);
			RMContainer rmContainer = application.allocate(type, node, priority, request, container);
			LOG.info("###### allocate container:" + application.getApplicationId().toString() + "->"
					+ containerId.toString());
			node.allocateContainer(rmContainer);
			increaseUsedResources(rmContainer);
		}
		// }
		return assignedContainers;
	}

	private void increaseUsedResources(RMContainer rmContainer) {
		Resources.addTo(usedResources, rmContainer.getAllocatedResource());
	}

	private int getMaxAllocatableContainers(FiCaSchedulerApp application, Priority priority, FiCaSchedulerNode node,
			NodeType type) {
		int maxContainers = 0;
		ResourceRequest offSwitchRequest = application.getResourceRequest(priority, ResourceRequest.ANY);
		if (offSwitchRequest != null) {
			maxContainers = offSwitchRequest.getNumContainers();
		}
		if (type == NodeType.OFF_SWITCH) {
			return maxContainers;
		}
		if (type == NodeType.RACK_LOCAL) {
			ResourceRequest rackLocalRequest = application.getResourceRequest(priority, node.getRMNode().getRackName());
			if (rackLocalRequest == null)
				return maxContainers;
			maxContainers = Math.min(maxContainers, rackLocalRequest.getNumContainers());
		}
		if (type == NodeType.NODE_LOCAL) {
			ResourceRequest nodeLocalRequest = application.getResourceRequest(priority,
					node.getRMNode().getNodeAddress());
			if (nodeLocalRequest != null)
				maxContainers = Math.min(maxContainers, nodeLocalRequest.getNumContainers());
		}
		return maxContainers;
	}

	private void containerLaunchedOnNode(ContainerId containerId, FiCaSchedulerNode node) {
		FiCaSchedulerApp application = this.getCurrentAttemptForContainer(containerId);
		if (application == null) {
			LOG.info("Unknown application " + containerId.getApplicationAttemptId().getAttemptId()
					+ " launched container " + containerId + " on node " + node);
			this.rmContext.getDispatcher().getEventHandler()
					.handle(new RMNodeCleanContainerEvent(node.getNodeID(), containerId));
			return;
		}
		application.containerLaunchedOnNode(containerId, node.getNodeID());
	}

	private void removeApplicationAttempt(ApplicationAttemptId applicationAttemptId,
			RMAppAttemptState rmAppAttemptFinalState, boolean keepContainers) throws IOException {
		FiCaSchedulerApp attempt = getApplicationAttempt(applicationAttemptId);
		SchedulerApplication<FiCaSchedulerApp> application = applications.get(applicationAttemptId.getApplicationId());
		if (application == null || attempt == null) {
			throw new IOException("Unknown application " + applicationAttemptId + " has completed!");
		}
		for (RMContainer container : attempt.getLiveContainers()) {
			if (keepContainers && container.getState().equals(RMContainerState.RUNNING)) {
				LOG.info("Skip killing " + container.getContainerId());
				continue;
			}
			containerCompleted(container, SchedulerUtils.createAbnormalContainerStatus(container.getContainerId(),
					SchedulerUtils.COMPLETED_APPLICATION), RMContainerEventType.KILL);
		}
		attempt.stop(rmAppAttemptFinalState);

		/**
		 * added by ericson at 2015-07-15
		 */
		synchronized (this.jobinfos) {
			this.jobinfos.get(applicationAttemptId.getApplicationId()).setApplicationAttemptid(null);
			this.jobinfos.get(applicationAttemptId.getApplicationId()).setJobinfoComplete(false);
		}
	}

	@Lock(AdaptiveScheduler.class)
	private synchronized void containerCompleted(RMContainer rmContainer, ContainerStatus containerStatus,
			RMContainerEventType event) {
		if (rmContainer == null) {
			return;
		}
		Container container = rmContainer.getContainer();
		FiCaSchedulerApp application = getCurrentAttemptForContainer(container.getId());
		ApplicationId appId = container.getId().getApplicationAttemptId().getApplicationId();
		FiCaSchedulerNode node = getNode(container.getNodeId());
		if (application == null) {
			LOG.info("Unknown application: " + appId + " released container " + container.getId() + " on node: " + node
					+ " with event: " + event);
			return;
		}
		application.containerCompleted(rmContainer, containerStatus, event);
		node.releaseContainer(container);
		Resources.subtractFrom(usedResources, container.getResource());

		/**
		 * added by ericson at 2015-07-16
		 */
		synchronized (this.jobinfos) {
			LOG.info("###### " + application.getApplicationId() + ":" + rmContainer.getContainerId().toString());
			JobInfo jobinfo = this.jobinfos.get(appId);
		}
	}

	private void addApplicationAttempt(ApplicationAttemptId appAttemptId, boolean transferStateFromPreviousAttempt,
			boolean shouldNotifyAttemptAdded) {
		SchedulerApplication<FiCaSchedulerApp> application = applications.get(appAttemptId.getApplicationId());
		String user = application.getUser();
		FiCaSchedulerApp schedulerApp = new FiCaSchedulerApp(appAttemptId, user, this.queue,
				this.queue.getActiveUsersManager(), this.rmContext);
		if (transferStateFromPreviousAttempt) {
			schedulerApp.transferStateFromPreviousAttempt(application.getCurrentAppAttempt());
		}
		application.setCurrentAppAttempt(schedulerApp);
		this.queue.getMetrics().submitAppAttempt(user);

		/**
		 * added by ericson at 2015-07-15
		 */
		synchronized (this.jobinfos) {
			LOG.info("###### begin add application attempt");
			if (this.jobinfos.containsKey(schedulerApp.getApplicationId())) {
				LOG.info("###### added application attempt");
				this.jobinfos.get(schedulerApp.getApplicationId()).setApplicationAttemptid(appAttemptId);
			} else {
				JobInfo jobinfo = new JobInfo(schedulerApp.getApplicationId(), appAttemptId);
				this.jobinfos.put(schedulerApp.getApplicationId(), jobinfo);
			}
		}

		// synchronized (this.applicationinfos) {
		// if
		// (this.applicationinfos.containsKey(schedulerApp.getApplicationId()))
		// {
		// this.applicationinfos.get(schedulerApp.getApplicationId()).setCurrentAttemptId(appAttemptId);
		// } else {
		// ApplicationInfo appinfo = new
		// ApplicationInfo(schedulerApp.getApplicationId(), appAttemptId);
		// this.applicationinfos.put(schedulerApp.getApplicationId(), appinfo);
		// }
		// }
		if (shouldNotifyAttemptAdded) {
			this.rmContext.getDispatcher().getEventHandler()
					.handle(new RMAppAttemptEvent(appAttemptId, RMAppAttemptEventType.ATTEMPT_ADDED));
		}
	}

	private void removeApplication(ApplicationId applicationId, RMAppState finalState) {
		SchedulerApplication<FiCaSchedulerApp> application = applications.get(applicationId);
		if (application == null) {
			LOG.warn("Couldn't find application " + applicationId);
			return;
		}
		this.queue.getActiveUsersManager().deactivateApplication(application.getUser(), applicationId);
		application.stop(finalState);
		applications.remove(applicationId);

		/**
		 * added by ericson at 2015-07-15
		 */
		synchronized (this.jobinfos) {
			this.jobinfos.remove(applicationId);
			this.initialize = false;
		}

		// synchronized (this.applicationinfos) {
		// this.applicationinfos.remove(applicationId);
		// }

		// migration code from MRv1
		// synchronized (AdaptiveScheduler.this) {
		// JobInfo jinfo = jobs.get(application);
		// jobs.remove(application);
		// jobsByID.remove(applicationId);
		// // computePlacement();
		// }
	}

	private void addApplication(ApplicationId applicationId, String queue, String user) {
		SchedulerApplication<FiCaSchedulerApp> application = new SchedulerApplication<FiCaSchedulerApp>(this.queue,
				user);
		this.applications.put(applicationId, application);
		this.queue.getMetrics().submitApp(user);
		LOG.info("Accepted application " + applicationId + " from user: " + user + ", currently num of applications: "
				+ applications.size());

		// synchronized (this.applicationinfos) {
		// ApplicationInfo appinfo = new ApplicationInfo(applicationId);
		// this.applicationinfos.put(applicationId, appinfo);
		// }

		// migration code from MRv1
		// synchronized (AdaptiveScheduler.this) {
		// JobInfo jinfo = new JobInfo(application.getCurrentAppAttempt());
		// jobs.put(application.getCurrentAppAttempt(), jinfo);
		// jobsByID.put(applicationId.toString(), jinfo);
		// pendingEvent = true;
		// // computePlacement();
		// }

		/**
		 * added by ericson at 2015-07-15
		 */
		synchronized (this.jobinfos) {
			LOG.info("###### being add application");
			if (!this.jobinfos.containsKey(applicationId)) {
				LOG.info("###### added application");
				JobInfo jobinfo = new JobInfo(applicationId);
				this.jobinfos.put(applicationId, jobinfo);
			}
		}

		this.rmContext.getDispatcher().getEventHandler()
				.handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));

	}

	// public void computePlacement() {
	// LOG.info("-----------------AS:computePlacement");
	// float utility;
	// float baseUtility;
	// float prevUtility;
	// ClusterOutline clusterClone;
	// synchronized (this) {
	// Iterator<Map.Entry<FiCaSchedulerApp, JobInfo>> iter =
	// jobs.entrySet().iterator();
	// while (iter.hasNext()) {
	// Entry<FiCaSchedulerApp, JobInfo> entry = iter.next();
	// FiCaSchedulerApp job = entry.getKey();
	// RMAppState runState =
	// this.rmContext.getRMApps().get(job.getApplicationId()).getState();
	// if (runState == RMAppState.FINISHED || runState == RMAppState.FAILED ||
	// runState == RMAppState.KILLED) {
	// iter.remove();
	// jobsByID.remove(job.getApplicationId());
	// }
	// }
	//
	// cleanup(this.cluster);
	// clusterClone = new ClusterOutline(this.cluster);
	// }
	//
	// placeReduces(clusterClone, jobs);
	// }

	// public void placeReduces(ClusterOutline co, Map<FiCaSchedulerApp,
	// JobInfo> jobs) {
	// for (JobInfo jinfo : jobs.values()) {
	// if (jinfo != null) {
	// FiCaSchedulerApp job = jinfo.getJobInProgress();
	// String jid = job.getApplicationId().toString();
	//
	// }
	// }
	// }

	// public void cleanup(ClusterOutline co) {
	// for (TrackerInfo tinfo : co.getOutline().values()) {
	// Assignment assignment = tinfo.getAssignment();
	// for (String jid : assignment.getJobs()) {
	// if (!jobsByID.containsKey(jid)) {
	// assignment.clean(jid);
	// } else {
	// FiCaSchedulerApp job = jobsByID.get(jid).getJobInProgress();
	// if (!job.isPending()) {
	// assignment.clean(jid);
	// }
	// }
	// }
	// }
	// co.taint();
	// }

	private void removeNode(RMNode removenode) {
		FiCaSchedulerNode node = getNode(removenode.getNodeID());
		if (node == null)
			return;
		for (RMContainer container : node.getRunningContainers()) {
			// 删除当前node运行的Container
			containerCompleted(container, SchedulerUtils.createAbnormalContainerStatus(container.getContainerId(),
					SchedulerUtils.LOST_CONTAINER), RMContainerEventType.KILL);
		}
		this.nodes.remove(removenode.getNodeID());
		Resources.subtractFrom(clusterResource, node.getRMNode().getTotalCapability());

		/**
		 * added by ericson at 2015-07-15
		 */
		// synchronized (this.nodeResources) {
		// this.nodeResources.remove(removenode.getNodeID());
		// }
	}

	private void addNode(RMNode nodeManager) {
		this.nodes.put(nodeManager.getNodeID(), new FiCaSchedulerNode(nodeManager, usePortForNodeName));
		Resources.addTo(this.clusterResource, nodeManager.getTotalCapability());

		// fill the resources on each node
		// this.allResourceArray.put(nodeManager.getNodeID(),
		// nodeManager.getTotalCapability());

		/**
		 * added by ericson at 2015-07-15
		 */
		// synchronized (this.nodeResources) {
		// this.nodeResources.put(nodeManager.getNodeID(),
		// nodeManager.getTotalCapability());
		// }

	}

	private FiCaSchedulerNode getNode(NodeId nodeId) {
		return this.nodes.get(nodeId);
	}

	@Override
	public void recover(RMState state) throws Exception {
		// TODO Auto-generated method stub
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return this.conf;
	}

	public Resource getUsedResource() {
		return this.usedResources;
	}

	// Migration code from MRv1
	// public class JobInfo {
	// private FiCaSchedulerApp job;
	// private JobProfile profile;
	// private JobProfilePerMap copyProfile;
	// private int assignedReduces = 0;
	// private float slots = 0.0f;
	// private long timestamp = 0;
	//
	// public JobInfo(FiCaSchedulerApp job) {
	// this.job = job;
	//
	// }
	//
	// public FiCaSchedulerApp getJobInProgress() {
	// return this.job;
	// }
	//
	// public float getUtility(int tasks, int reduceTasks) {
	// return 0.0f;
	// }
	//
	// public JobProfile getProfile() {
	// return profile;
	// }
	// }

	public class JobProfile {
		private int cpu;
		private int io;
		private int mem;

		public JobProfile(int cpu, int io, int mem) {
			this.cpu = cpu;
			this.io = io;
			this.mem = mem;
		}

		public int getCpuUsage() {
			return cpu;
		}

		public int getIoUsage() {
			return io;
		}

		public int getMemUsage() {
			return mem;
		}
	}

	public class JobProfilePerMap {
		private float cpu;
		private float io;
		private float mem;

		public JobProfilePerMap(float cpu, float io, float mem) {
			this.cpu = cpu;
			this.io = io;
			this.mem = mem;
		}

		public float getCpuUsage() {
			return cpu;
		}

		public float getIoUsage() {
			return io;
		}

		public float getMemUsage() {
			return mem;
		}
	}
}