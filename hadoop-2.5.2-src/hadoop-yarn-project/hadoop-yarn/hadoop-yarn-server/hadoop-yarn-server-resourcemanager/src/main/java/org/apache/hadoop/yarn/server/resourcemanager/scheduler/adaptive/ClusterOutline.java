package org.apache.hadoop.yarn.server.resourcemanager.scheduler.adaptive;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

public class ClusterOutline {
	private static final Log LOG = LogFactory
			.getLog("org.apache.hadoop.yarn.server.resourcemanager.scheduler.adaptive.ClusterOutline");
	private Map<RMNode, TrackerInfo> outline = new HashMap<RMNode, TrackerInfo>();
	private Map<String, Integer> count = new HashMap<String, Integer>();
	private Map<String, Integer> reduceCount = new HashMap<String, Integer>();
	private PriorityQueue<TrackerInfo> trackersByReduce = null;
	private boolean tainted = false;

	public class TrackerReduceComparator implements Comparator<TrackerInfo> {

		@Override
		public int compare(TrackerInfo o1, TrackerInfo o2) {
			// TODO Auto-generated method stub
			int e1 = o1.getAssignment().getNumReduces();
			int e2 = o2.getAssignment().getNumReduces();
			if (e1 != e2)
				return Integer.compare(e1, e2);
			Set<String> reduce1 = o1.getAssignment().getJobs(TaskType.Reduce);
			int reduceJobCount1 = reduce1 == null ? 0 : reduce1.size();
			Set<String> reduce2 = o2.getAssignment().getJobs(TaskType.Reduce);
			int reduceJobCount2 = reduce2 == null ? 0 : reduce2.size();
			return Integer.compare(reduceJobCount1, reduceJobCount2);
		}

	}

	public ClusterOutline() {

	}

	public void taint() {
		this.tainted = true;
	}

	public PriorityQueue<TrackerInfo> getTrackersByReduce() {
		if (!tainted && this.trackersByReduce != null)
			return this.trackersByReduce;
		this.trackersByReduce = new PriorityQueue<TrackerInfo>(21, new TrackerReduceComparator());
		for (TrackerInfo tinfo : outline.values()) {
			this.trackersByReduce.add(tinfo);
		}
		return this.trackersByReduce;
	}

	public Map<RMNode, TrackerInfo> getOutline() {
		return outline;
	}

//	public ClusterOutline(ClusterOutline co) {
//		Cloner cloner = new Cloner();
//		for (Entry<RMNode, TrackerInfo> entry : co.getOutline().entrySet()) {
//			RMNode tracker = entry.getKey();
//			TrackerInfo tinfo = entry.getValue();
//			this.outline.put(tracker, cloner.deepClone(tinfo));
//		}
//		this.count = cloner.deepClone(co.getCount());
//		this.reduceCount = cloner.deepClone(co.getReduceCount());
//		this.trackersByReduce = cloner.deepClone(co.trackersByReduce);
//		this.tainted = cloner.deepClone(co.tainted);
//	}

	public Map<String, Integer> getCount() {
		updateCount();
		return count;
	}

	public Map<String, Integer> getReduceCount() {
		updateCount();
		return reduceCount;
	}

	public TrackerInfo updateTracker(RMNode tracker) {
		if (!outline.containsKey(tracker))
			outline.put(tracker, new TrackerInfo(tracker));
		TrackerInfo tinfo = outline.get(tracker);
		tinfo.setResourceStatus(tracker.getTotalCapability());
		return tinfo;
	}

	public void updateCount() {
		if (!tainted)
			return;
		count = new HashMap<String, Integer>();
		reduceCount = new HashMap<String, Integer>();
		for (TrackerInfo tinfo : outline.values()) {
			for (Entry<String, Integer> entry : tinfo.getAssignment().getMaps()) {
				String jid = entry.getKey();
				Integer num = entry.getValue();
				if (!count.containsKey(jid))
					count.put(jid, 0);
				count.put(jid, count.get(jid) + num);
			}
			for (Entry<String, Integer> entry : tinfo.getAssignment().getReduces()) {
				String jid = entry.getKey();
				Integer num = entry.getValue();
				if (!reduceCount.containsKey(jid))
					reduceCount.put(jid, 0);
				reduceCount.put(jid, reduceCount.get(jid) + num);
			}
		}
	}

	public float getUtility(Map<FiCaSchedulerApp, JobInfo> jobs) {
		if (count.size() == 0 && reduceCount.size() == 0)
			return -100.0f;
		float utility = 0.0f;
		for (Entry<FiCaSchedulerApp, JobInfo> entry : jobs.entrySet()) {
			FiCaSchedulerApp job = entry.getKey();
			JobInfo jinfo = entry.getValue();
			String jid = job.getApplicationId().toString();
			int num = 0;
			if (count.containsKey(jid))
				num = count.get(jid);
			int numReduce = 0;
			if (reduceCount.containsKey(jid))
				num = count.get(jid);
//			utility += jinfo.getUtility(num, numReduce);
		}
		return utility;
	}

	public String toString() {
		String s = new String();
		s += "- type: cluster\n";
		s += "  time: " + System.currentTimeMillis() + "\n";
		if (outline.entrySet().size() > 0)
			s += "  trackers:\n";
		for (Entry<RMNode, TrackerInfo> entry : outline.entrySet()) {
			RMNode tracker = entry.getKey();
			TrackerInfo tinfo = entry.getValue();
			Assignment assign = tinfo.getAssignment();
			s += "  - id: " + tracker.getNodeID().toString() + "\n";
			s += "    a: " + assign.toString() + "\n";
			// s += " r: { cpu: " + assign.getCpu() + ", io: " +
			// assign.getIo() + " }\n";
		}
		return s;
	}

}
