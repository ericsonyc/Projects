package org.apache.hadoop.yarn.server.resourcemanager.scheduler.adaptive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;

enum TaskType {
	Map, Reduce
}

public class Assignment implements Iterable<Entry<String, Integer>> {
	private static final TaskType[] TaskTypes = new TaskType[] { TaskType.Map, TaskType.Reduce };
	private Map<String, Integer> map = new ConcurrentHashMap<String, Integer>();
	private Map<String, Integer> reduce = new ConcurrentHashMap<String, Integer>();
	private Map<String, Integer> containers = new ConcurrentHashMap<String, Integer>();
	private float cpu = 0;
	private float io = 0;

	public int getNumTasks(TaskType type) {
		int numTasks = 0;
		if (type == TaskType.Map)
			numTasks = getNumMaps();
		else
			numTasks = getNumReduces();
		return numTasks;
	}
	
	public int getNumContainers(){
		int numContainers=0;
		for(Integer num:containers.values())
			numContainers+=num;
		return numContainers;
	}

	public int getNumMaps() {
		int numTasks = 0;
		for (Integer num : map.values())
			numTasks += num;
		return numTasks;
	}

	public int getNumReduces() {
		int numTasks = 0;
		for (Integer num : reduce.values())
			numTasks += num;
		return numTasks;
	}

	public void put(String jid, TaskType type) {
		int numTasks = 1;
		if (type == TaskType.Map) {
			if (map.containsKey(jid))
				numTasks += map.get(jid);
			map.put(jid, numTasks);
		} else {
			if (reduce.containsKey(jid))
				numTasks += reduce.get(jid);
			reduce.put(jid, numTasks);
		}
	}

	public void put(String jid) {
		int numContainers = 1;
		if (containers.containsKey(jid)) {
			numContainers += containers.get(jid);
		}
		containers.put(jid, numContainers);
	}

	public void put(String jid, int numContainers) {
		containers.put(jid, numContainers);
	}

	public void put(String jid, TaskType type, int numTasks) {
		if (type == TaskType.Map)
			map.put(jid, numTasks);
		else
			reduce.put(jid, numTasks);
	}

	public int get(String jid, TaskType type) {
		Integer numTasks = null;
		if (type == TaskType.Map)
			numTasks = map.get(jid);
		else
			numTasks = reduce.get(jid);
		return numTasks == null ? 0 : numTasks;
	}

	public Set<String> getJobs(TaskType type) {
		return type == TaskType.Map ? map.keySet() : reduce.keySet();
	}

	public Set<String> getJobs() {
		return containers.keySet();
	}

	// public Assignment clone() {
	// Cloner cloner = new Cloner();
	// return cloner.deepClone(this);
	// }

	public void clean(String jid) {
		map.remove(jid);
		reduce.remove(jid);
	}

	public void cleanMaps(String jid) {
		map.remove(jid);
	}

	public void cleanReduces(String jid) {
		reduce.remove(jid);
	}

	public Assignment remove(int n) {
		Assignment removed = new Assignment();
		while (n > 0) {
			Map.Entry<String, Integer> entry = map.entrySet().iterator().next();
			int numTasks = entry.getValue() - 1;
			map.put(entry.getKey(), numTasks);
			if (numTasks == 0)
				map.remove(entry.getKey());
			n -= 1;
			removed.put(entry.getKey(), TaskType.Map);
		}
		return removed;
	}

	public Assignment smartRemove(int n, ArrayList<String> leastNeedy) {
		Assignment removed = new Assignment();
		int i = 0;
		String jid;
		while (n > 0) {
			for (int j = i; j < leastNeedy.size(); j++) {
				jid = leastNeedy.get(j);
				if (map.containsKey(jid)) {
					int numTasks = map.get(jid) - 1;
					map.put(jid, numTasks);
					if (numTasks == 0)
						map.remove(jid);
					n -= 1;
					removed.put(jid, TaskType.Map);
					break;
				}
				i++;
			}
		}
		return removed;
	}

	public Assignment toInclude(Assignment desired) {
		Assignment include = new Assignment();
		for (TaskType type : TaskTypes) {
			Collection<String> currentJobs = new TreeSet<String>(getJobs(type));
			Collection<String> nextJobs = new TreeSet<String>(desired.getJobs(type));
			Collection<String> keepJobs = new TreeSet<String>(desired.getJobs(type));
			nextJobs.removeAll(currentJobs);
			keepJobs.retainAll(currentJobs);
			for (String jid : nextJobs) {
				include.put(jid, type, desired.get(jid, type));
			}
			for (String jid : keepJobs) {
				int numTasks = desired.get(jid, type) - get(jid, type);
				if (numTasks > 0)
					include.put(jid, type, numTasks);
			}
		}
		return include;
	}

	@Override
	public Iterator<Entry<String, Integer>> iterator() {
		// TODO Auto-generated method stub
		Set<Entry<String, Integer>> taskSet = new HashSet<Entry<String, Integer>>();
		taskSet.addAll(map.entrySet());
		return taskSet.iterator();
	}

	public Set<Entry<String, Integer>> getMaps() {
		Set<Entry<String, Integer>> taskSet = new HashSet<Entry<String, Integer>>();
		taskSet.addAll(map.entrySet());
		return taskSet;
	}

	public Set<Entry<String, Integer>> getContainers() {
		Set<Entry<String, Integer>> taskSet = new HashSet<Entry<String, Integer>>();
		taskSet.addAll(containers.entrySet());
		return taskSet;
	}

	public Set<Entry<String, Integer>> getReduces() {
		Set<Entry<String, Integer>> taskSet = new HashSet<Entry<String, Integer>>();
		taskSet.addAll(reduce.entrySet());
		return taskSet;
	}

	public float getCpu() {
		return cpu;
	}

	public float getIo() {
		return io;
	}

	public void setCpu(float cpu) {
		this.cpu = cpu;
	}

	public void setIo(float io) {
		this.io = io;
	}

	public String toString() {
		String s = new String();
		ArrayList<String> maps = new ArrayList<String>();
		ArrayList<String> reduces = new ArrayList<String>();
		for (Entry<String, Integer> entry : map.entrySet())
			maps.add(entry.getKey() + ": " + entry.getValue());
		for (Entry<String, Integer> entry : reduce.entrySet())
			reduces.add(entry.getKey() + ": " + entry.getValue());
		return "{ m: { " + StringUtils.join(maps, ",") + " }, " + "r: { " + StringUtils.join(reduces, ",") + "} }";
	}

}
