package org.apache.hadoop.yarn.server.resourcemanager.scheduler.adaptive;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

public class ContainerInfo {
	private ApplicationId applicationId = null;
	private ApplicationAttemptId appAttemptId = null;
	private int containerId;
	private ResourceRequest request = null;
	private boolean isAppMaster = false;

	public ContainerInfo(ApplicationId applicationId, ApplicationAttemptId appAttemptId, int containerId,
			ResourceRequest request) {
		this.applicationId = applicationId;
		this.appAttemptId = appAttemptId;
		this.containerId = containerId;
		this.request = request;
		if (request.getPriority().getPriority() == 0)
			isAppMaster = true;
	}

	public ApplicationId getApplicationId() {
		return applicationId;
	}

	public void setApplicationId(ApplicationId applicationId) {
		this.applicationId = applicationId;
	}

	public boolean isAppMaster() {
		return isAppMaster;
	}

	public void setAppMaster(boolean isAppMaster) {
		this.isAppMaster = isAppMaster;
	}

	public ApplicationAttemptId getAppAttemptId() {
		return appAttemptId;
	}

	public void setAppAttemptId(ApplicationAttemptId appAttemptId) {
		this.appAttemptId = appAttemptId;
	}

	public int getContainerId() {
		return containerId;
	}

	public void setContainerId(int containerId) {
		this.containerId = containerId;
	}

	public ResourceRequest getRequest() {
		return request;
	}

	public void setRequest(ResourceRequest request) {
		this.request = request;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.applicationId + "#" + this.appAttemptId + "#" + this.containerId;
	}

}
