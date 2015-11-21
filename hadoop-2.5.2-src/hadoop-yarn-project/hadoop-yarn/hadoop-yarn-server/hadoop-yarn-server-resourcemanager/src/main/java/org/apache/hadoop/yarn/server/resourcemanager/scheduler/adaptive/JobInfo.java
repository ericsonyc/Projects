package org.apache.hadoop.yarn.server.resourcemanager.scheduler.adaptive;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

public class JobInfo {
	// preserve the applicationId
	private ApplicationId applicationid;
	// preserve the application attempt id
	private ApplicationAttemptId applicationAttemptid;
	// preserve whether it is complete
	private boolean jobinfoComplete = false;
	// preserve the AppMaster resource request
	private ContainerProfile appMasterProfile;
	private int appMasterNum;
	// preserve the map resource request
	private ContainerProfile mapProfile;
	private int mapNum;
	// preserve the reduce request
	private ContainerProfile reduceProfile;
	private int reduceNum;

	private ContainerProfile other;

	// preserve the running container tags
	private int runningAppMasters = 0;
	private int runningMaps = 0;
	private int runningReduces = 0;
	// preserve the completed container tags
	private int completedAppMasters = 0;
	private int completedRunningMaps = 0;
	private int completedRunningReduces = 0;

	public JobInfo(ApplicationId applicationid) {
		this.applicationid = applicationid;
	}

	public JobInfo(ApplicationId applicationid, ApplicationAttemptId applicationAttemptid) {
		this.applicationid = applicationid;
		this.applicationAttemptid = applicationAttemptid;
	}

	public ContainerProfile getOther() {
		return other;
	}

	public void setOther(ContainerProfile other) {
		this.other = other;
	}

	public ApplicationId getApplicationid() {
		return applicationid;
	}

	public void setApplicationid(ApplicationId applicationid) {
		this.applicationid = applicationid;
	}

	public ApplicationAttemptId getApplicationAttemptid() {
		return applicationAttemptid;
	}

	public void setApplicationAttemptid(ApplicationAttemptId applicationAttemptid) {
		this.applicationAttemptid = applicationAttemptid;
		this.jobinfoComplete = true;
	}

	public boolean isJobinfoComplete() {
		return jobinfoComplete;
	}

	public void setJobinfoComplete(boolean jobinfoComplete) {
		this.jobinfoComplete = jobinfoComplete;
	}

	public ContainerProfile getAppMasterProfile() {
		return appMasterProfile;
	}

	public void setAppMasterProfile(ContainerProfile appMasterProfile) {
		this.appMasterProfile = appMasterProfile;
		if (appMasterProfile.getNumberOfContainer() > appMasterNum)
			appMasterNum = appMasterProfile.getNumberOfContainer();
	}

	public int getAppMasterNum() {
		return appMasterNum;
	}

	public void setAppMasterNum(int appMasterNum) {
		this.appMasterNum = appMasterNum;
	}

	public ContainerProfile getMapProfile() {
		return mapProfile;
	}

	public void setMapProfile(ContainerProfile mapProfile) {
		this.mapProfile = mapProfile;
		if (mapProfile.getNumberOfContainer() > mapNum)
			mapNum = mapProfile.getNumberOfContainer();
	}

	public int getMapNum() {
		return mapNum;
	}

	public void setMapNum(int mapNum) {
		this.mapNum = mapNum;
	}

	public ContainerProfile getReduceProfile() {
		return reduceProfile;
	}

	public void setReduceProfile(ContainerProfile reduceProfile) {
		this.reduceProfile = reduceProfile;
		if (reduceProfile.getNumberOfContainer() > reduceNum) {
			reduceNum = reduceProfile.getNumberOfContainer();
		}
	}

	public int getReduceNum() {
		return reduceNum;
	}

	public void setReduceNum(int reduceNum) {
		this.reduceNum = reduceNum;
	}

	public int getRunningAppMasters() {
		return runningAppMasters;
	}

	public void setRunningAppMasters(int runningAppMasters) {
		this.runningAppMasters = runningAppMasters;
	}

	public int getRunningMaps() {
		return runningMaps;
	}

	public void setRunningMaps(int runningMaps) {
		this.runningMaps = runningMaps;
	}

	public int getRunningReduces() {
		return runningReduces;
	}

	public void setRunningReduces(int runningReduces) {
		this.runningReduces = runningReduces;
	}

	public int getCompletedAppMasters() {
		return completedAppMasters;
	}

	public void setCompletedAppMasters(int completedAppMasters) {
		this.completedAppMasters = completedAppMasters;
	}

	public int getCompletedRunningMaps() {
		return completedRunningMaps;
	}

	public void setCompletedRunningMaps(int completedRunningMaps) {
		this.completedRunningMaps = completedRunningMaps;
	}

	public int getCompletedRunningReduces() {
		return completedRunningReduces;
	}

	public void setCompletedRunningReduces(int completedRunningReduces) {
		this.completedRunningReduces = completedRunningReduces;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		String appMaster = "";
		if (this.appMasterProfile != null)
			appMaster = this.appMasterProfile.toString();
		String map = "";
		if (this.mapProfile != null)
			map = this.mapProfile.toString();
		String reduce = "";
		if (this.reduceProfile != null)
			reduce = this.reduceProfile.toString();
		return this.applicationid.toString() + ":" + appMaster + ":" + map + ":" + reduce;
	}

}
