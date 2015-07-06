package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

//basic class for storing capacity per label
public class NodeLabelQueueEntitlement extends QueueEntitlement {
	// define 2 maps : cap and maxcap maps
	private static final String NL = CommonNodeLabelsManager.NO_LABEL;
	private Map<String, Float> capmap;
	private Map<String, Float> maxcapmap;
	
	private void init() {
		// all object initialization goes here
		capmap = new HashMap<String, Float>();
		maxcapmap = new HashMap<String, Float>();
	}
	
	public NodeLabelQueueEntitlement() {
		init();
	}

	public NodeLabelQueueEntitlement(float capacity, float maxCapacity) {
		super(capacity, maxCapacity);
		init();
		setCapacity(capacity);
		setMaxCapacity(maxCapacity);
	}

	public void setMaxCapacity(String label, float maxCapacity) {
		this.maxcapmap.put(label, maxCapacity);
	}
	
	@Override
	public void setMaxCapacity(float maxCapacity) {
		setMaxCapacity(NL, maxCapacity);
	}
	
	public void setCapacity(String label, float capacity) {
		this.capmap.put(label, capacity);
	}
	
	@Override
	public void setCapacity(float capacity) {
		setCapacity(NL, capacity);
	}

	public float getCapacity(String label) {
		Float res = this.capmap.get(label);
		if (res == null) {
			return Float.NaN;
		}
		return res.floatValue();
	}
	
	@Override
	public float getCapacity() {
		return getCapacity(NL);
	}
	
	public float getMaxCapacity(String label) {
		Float res = this.maxcapmap.get(label);
		if (res == null) {
			return Float.NaN;
		}
		return res.floatValue();
	}
	
	@Override
	public float getMaxCapacity() {
		return getMaxCapacity(NL);
	}

}
