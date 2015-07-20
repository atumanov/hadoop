/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * An in memory implementation of a reservation allocation using the
 * {@link RLESparseResourceAllocation}
 * 
 */
class InMemoryReservationAllocation implements ReservationAllocation {

  private final String planName;
  private final ReservationId reservationID;
  private final String user;
  private final ReservationDefinition contract;
  protected long startTime;
  protected long endTime;
  private final Map<ReservationInterval, Resource> allocationRequests;
  private boolean hasGang = false;
  private long acceptedAt = -1;
  private final String nodeLabel;

  protected RLESparseResourceAllocation resourcesOverTime;

  InMemoryReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<ReservationInterval, Resource> allocationRequests,
      ResourceCalculator calculator, Resource minAlloc) {
    this(reservationID, contract, user, planName, startTime, endTime,
        allocationRequests, calculator, minAlloc, RMNodeLabelsManager.NO_LABEL,
        false);
  }

  InMemoryReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<ReservationInterval, Resource> allocationRequests,
      ResourceCalculator calculator, Resource minAlloc, boolean hasGang) {
    this(reservationID, contract, user, planName, startTime, endTime,
        allocationRequests, calculator, minAlloc, RMNodeLabelsManager.NO_LABEL,
        hasGang);

  }

  InMemoryReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<ReservationInterval, Resource> allocationRequests,
      ResourceCalculator calculator, Resource minAlloc, String nodeLabel,
      boolean hasGang) {
    this.contract = contract;
    this.startTime = startTime;
    this.endTime = endTime;
    this.reservationID = reservationID;
    this.user = user;
    this.allocationRequests = allocationRequests;
    this.planName = planName;
    resourcesOverTime = new RLESparseResourceAllocation(calculator, minAlloc);
    this.nodeLabel = nodeLabel;
    if (allocationRequests != null) {
      for (Map.Entry<ReservationInterval, Resource> r : allocationRequests
          .entrySet()) {
        resourcesOverTime.addInterval(r.getKey(), r.getValue());
      }
    }
    this.hasGang = hasGang;
  }

  @Override
  public ReservationId getReservationId() {
    return reservationID;
  }

  @Override
  public ReservationDefinition getReservationDefinition() {
    return contract;
  }

  @Override
  public long getStartTime() {
    return getStartTime(RMNodeLabelsManager.NO_LABEL);
  }

  @Override
  public long getEndTime() {
    return getEndTime(RMNodeLabelsManager.NO_LABEL);
  }
  
  @Override
  public long getStartTime(String label) {
    if(nodeLabel.equals(label)){
      return startTime;
    } else {
      return -1;
    }
  }

  @Override
  public long getEndTime(String label) {
    if(nodeLabel.equals(label)){
      return endTime;
    } else {
      return -1;
    }
  }
  
  @Override
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  @Override
  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  @Override
  public Map<ReservationInterval, Resource> getAllocationRequests() {
    return Collections.unmodifiableMap(allocationRequests);
  }

  @Override
  public String getPlanName() {
    return planName;
  }

  @Override
  public String getUser() {
    return user;
  }
  
  @Override
  public void setHasGang(boolean hasGang) {
   this.hasGang = hasGang; 
  }
  
  @Override
  public boolean containsGangs() {
    return hasGang;
  }

  @Override
  public void setAcceptanceTimestamp(long acceptedAt) {
    this.acceptedAt = acceptedAt;
  }

  @Override
  public long getAcceptanceTime() {
    return acceptedAt;
  }

  @Override
  public Resource getResourcesAtTime(long tick) {
    if (tick < startTime || tick >= endTime) {
      return Resource.newInstance(0, 0);
    }
    return Resources.clone(resourcesOverTime.getCapacityAtTime(tick));
  }

  @Override
  public String toString() {
    StringBuilder sBuf = new StringBuilder();
    sBuf.append(getReservationId()).append(" label:").append(getNodeLabels().get(0)).append(" user:").append(getUser())
        .append(" startTime: ").append(getStartTime(nodeLabel)).append(" endTime: ")
        .append(getEndTime(nodeLabel)).append(" alloc:[")
        .append(resourcesOverTime.toString()).append("] ");
    return sBuf.toString();
  }

  @Override
  public int compareTo(ReservationAllocation other) {
    // reverse order of acceptance
    if (this.getAcceptanceTime() > other.getAcceptanceTime()) {
      return -1;
    }
    if (this.getAcceptanceTime() < other.getAcceptanceTime()) {
      return 1;
    }
    return 0;
  }

  @Override
  public int hashCode() {
    return reservationID.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    InMemoryReservationAllocation other = (InMemoryReservationAllocation) obj;
    return this.reservationID.equals(other.getReservationId());
  }

  @Override
  public Resource getResourcesAtTime(long t, String nodeLabel) {
    if(nodeLabel.equals(nodeLabel)){
      return getResourcesAtTime(t);
    } else {
      return null;
    }
  }

  @Override
  public Map<ReservationInterval, Resource> getAllocationRequests(
      String nodeLabel) {
    if(nodeLabel.equals(nodeLabel)){
      return getAllocationRequests();
    } else {
      return null;
    }
  }

  @Override
  public List<String> getNodeLabels() {
    return Collections.singletonList(nodeLabel);
  }

  @Override
  public Map<String, ReservationAllocation> getPerLabelAllocations() {
    Map<String, ReservationAllocation> map =
        new HashMap<String, ReservationAllocation>();
    map.put(nodeLabel, this);
    return map;
  }

}
