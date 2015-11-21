package org.apache.hadoop.yarn.server.resourcemanager.scheduler.adaptive;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

public class ApplicationRequestInfo {
	private ApplicationId applicationId;
	private ApplicationAttemptId appAttemptId;
	private ResourceRequest request;

	public ApplicationRequestInfo(ApplicationId applicationid, ApplicationAttemptId appAttemptId,
			ResourceRequest request) {
		this.applicationId = applicationid;
		this.appAttemptId = appAttemptId;
		this.request = request;
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (obj instanceof ApplicationRequestInfo) {
			ApplicationRequestInfo req = (ApplicationRequestInfo) obj;
			if (this.applicationId.equals(req.getApplicationId()) && this.appAttemptId.equals(req.getAppAttemptId())
					&& this.request.equals(req.getRequest()))
				return true;
		}
		return false;
	}

	public ApplicationId getApplicationId() {
		return applicationId;
	}

	public void setApplicationId(ApplicationId applicationId) {
		this.applicationId = applicationId;
	}

	public ApplicationAttemptId getAppAttemptId() {
		return appAttemptId;
	}

	public void setAppAttemptId(ApplicationAttemptId appAttemptId) {
		this.appAttemptId = appAttemptId;
	}

	public ResourceRequest getRequest() {
		return request;
	}

	public void setRequest(ResourceRequest request) {
		this.request = request;
	}
}
