package org.apache.hadoop.yarn.server.resourcemanager.scheduler.adaptive;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

public class TrackerInfo {
	private RMNode tracker;
	private Resource resources;
	private Assignment assignment = new Assignment();

	public TrackerInfo(RMNode tracker) {
		this.tracker = tracker;
	}

	public RMNode getTaskTracker() {
		return this.tracker;
	}

	public void setResourceStatus(Resource resources) {
		this.resources = resources;
	}

	public Resource getResourceStatus() {
		return this.resources;
	}

	public Assignment getAssignment() {
		return this.assignment;
	}

	public void setAssignment(Assignment newAssignment) {
		this.assignment = newAssignment;
	}
}
