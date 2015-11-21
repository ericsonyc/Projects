package org.apache.hadoop.yarn.server.resourcemanager.scheduler.adaptive;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.mortbay.log.Log;

public class ContainerProfile {
	// preserve the application id
	private ApplicationId applicationid;
	// preserve the number of this container
	private int numberOfContainer;
	// preserve the container resource request
	private ResourceRequest containerRequest;

	public ContainerProfile(ApplicationId applicationid, int numberOfContainer, ResourceRequest containerRequest) {
		this.applicationid = applicationid;
		this.numberOfContainer = numberOfContainer;
		this.containerRequest = containerRequest;
	}

	public ContainerProfile(ApplicationId applicationid, ResourceRequest containerRequest) {
		this.applicationid = applicationid;
		this.containerRequest = containerRequest;
		this.numberOfContainer = containerRequest.getNumContainers();
	}

	public ApplicationId getApplicationid() {
		return this.applicationid;
	}

	public void setApplicationid(ApplicationId applicationid) {
		this.applicationid = applicationid;
	}

	public int getNumberOfContainer() {
		return numberOfContainer;
	}

	public void setNumberOfContainer(int numberOfContainer) {
		this.numberOfContainer = numberOfContainer;
//		this.containerRequest.setNumContainers(numberOfContainer);
	}

	public ResourceRequest getContainerRequest() {
		return containerRequest;
	}

	public void setContainerRequest(ResourceRequest containerRequest) {
		this.containerRequest = containerRequest;
		this.setNumberOfContainer(containerRequest.getNumContainers());
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		// return "containerNum:" +
		// String.valueOf(this.containerRequest.getNumContainers());
		String appIDString = this.applicationid.toString();
		int priority = containerRequest.getPriority().getPriority();
		if (priority == 0 || priority == 10 || priority == 20)
			return appIDString + ":" + containerRequest.getPriority().toString();
		else
			return appIDString + ":" + String.valueOf(this.hashCode());
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (!(obj instanceof ContainerProfile))
			return false;
		// if (obj == null)
		// return false;
		// if (obj == this)
		// return true;
		// if (obj.getClass() != getClass())
		// return false;
		Log.info("-------------obj is ContainerProfile");
		ContainerProfile container = (ContainerProfile) obj;
		if (!container.getApplicationid().toString().equals(applicationid.toString())) {
			Log.info("----------------applicationid is not equal");
			return false;
		}
		int priority = getContainerRequest().getPriority().getPriority();
		if (priority != container.getContainerRequest().getPriority().getPriority()
				&& (priority == 0 || priority == 10 || priority == 20)) {
			Log.info("---------------priority is not equal");
			return false;
		}
		if (priority != 0 && priority != 10 && priority != 20)
			return false;
		Log.info("----------------return true");
		return true;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		int result = 17;
		int c = 0;
		c = this.getApplicationid().hashCode();
		result = 31 * result + c;
		c = this.getContainerRequest().getPriority().getPriority();
		if (c != 0 && c != 10 && c != 20)
			c = this.getContainerRequest().getPriority().hashCode();
		result = 31 * result + c;
		return result;
	}

}
