package org.apache.hadoop.yarn.server.resourcemanager.scheduler.adaptive;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

public class ApplicationInfo {
	// application id
	private ApplicationId appId;
	// current application attempt id
	private ApplicationAttemptId currentAttemptId;
	// appmaster resource request
	private ResourceRequest appMasterRequest;
	// map resource request
	private ResourceRequest mapRequest;
	// reduce resource request
	private ResourceRequest reduceRequest;
	// judge if it is appmaster
	private boolean isAppMaster;
	// judge if it is reduce
	private boolean isReduce;
	// preserve the pending map containers
	private int mapContainers;
	// presreve the pending reduce containers
	private int reduceContainers;
	// preserve the total map Containers
	private int totalMapContainers;
	// preserve the total reduce Containers
	private int totalReduceContainers;
	// preserve the appMaster containers' number
	private int totalAppMasterContainers;

	public int getTotalAppMasterContainers() {
		return totalAppMasterContainers;
	}

	public void setTotalAppMasterContainers(int totalAppMasterContainers) {
		this.totalAppMasterContainers = totalAppMasterContainers;
	}

	public int getMapContainers() {
		return mapContainers;
	}

	public void setMapContainers(int mapContainers) {
		this.mapContainers = mapContainers;
	}

	public int getReduceContainers() {
		return reduceContainers;
	}

	public void setReduceContainers(int reduceContainers) {
		this.reduceContainers = reduceContainers;
	}

	public ApplicationInfo(ApplicationId appId, ApplicationAttemptId appattemptId) {
		this(appId);
		this.currentAttemptId = appattemptId;
	}

	public ApplicationInfo(ApplicationId appId) {
		this.appId = appId;
		this.isAppMaster = false;
		this.isReduce = false;
		this.mapContainers = 0;
		this.reduceContainers = 0;
	}

	public ApplicationId getAppId() {
		return appId;
	}

	public void setAppId(ApplicationId appId) {
		this.appId = appId;
	}

	public ApplicationAttemptId getCurrentAttemptId() {
		return currentAttemptId;
	}

	public void setCurrentAttemptId(ApplicationAttemptId currentAttemptId) {
		this.currentAttemptId = currentAttemptId;
	}

	public ResourceRequest getAppMasterRequest() {
		return appMasterRequest;
	}

	public void setAppMasterRequest(ResourceRequest appMasterRequest) {
		this.appMasterRequest = appMasterRequest;
		this.totalAppMasterContainers = appMasterRequest.getNumContainers();
	}

	public ResourceRequest getMapRequest() {
		return mapRequest;
	}

	public void setMapRequest(ResourceRequest mapRequest) {
		this.mapRequest = mapRequest;
		this.totalMapContainers = mapRequest.getNumContainers();
	}

	public ResourceRequest getReduceRequest() {
		return reduceRequest;
	}

	public void setReduceRequest(ResourceRequest reduceRequest) {
		this.reduceRequest = reduceRequest;
		this.totalReduceContainers = reduceRequest.getNumContainers();
	}

	public boolean isAppMaster() {
		return isAppMaster;
	}

	public void setAppMaster(boolean isAppMaster) {
		this.isAppMaster = isAppMaster;
	}

	public boolean isReduce() {
		return isReduce;
	}

	public void setReduce(boolean isReduce) {
		this.isReduce = isReduce;
	}

	public int getTotalMapContainers() {
		return totalMapContainers;
	}

	public void setTotalMapContainers(int totalMapContainers) {
		this.totalMapContainers = totalMapContainers;
	}

	public int getTotalReduceContainers() {
		return totalReduceContainers;
	}

	public void setTotalReduceContainers(int totalReduceContainers) {
		this.totalReduceContainers = totalReduceContainers;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.appId + "#"
				+ this.currentAttemptId.toString().substring(this.currentAttemptId.toString().lastIndexOf('_'));
	}

}
