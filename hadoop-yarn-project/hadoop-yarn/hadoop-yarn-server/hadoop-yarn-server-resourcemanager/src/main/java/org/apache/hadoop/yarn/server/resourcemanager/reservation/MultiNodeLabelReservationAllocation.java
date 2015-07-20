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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * This class represent a ReservationAllocation that spans multiple NodeLabels.
 * To do so it contains multiple single-label ReservationAllocations
 */
class MultiNodeLabelReservationAllocation extends InMemoryReservationAllocation {

  private Map<String, ReservationAllocation> perLabelAllocations;

  public MultiNodeLabelReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      Map<String, ReservationAllocation> perLabelAllocations,
      ResourceCalculator calculator, Resource minAlloc) {
    super(reservationID, contract, user, planName, Long.MAX_VALUE,
        Long.MIN_VALUE, null, calculator, minAlloc);
    this.perLabelAllocations = perLabelAllocations;

    for (ReservationAllocation r : perLabelAllocations.values()) {
      setStartTime(Math.min(this.startTime, r.getStartTime(r.getNodeLabels().get(0))));
      setEndTime(Math.max(this.endTime, r.getEndTime(r.getNodeLabels().get(0))));
      if (r.containsGangs()) {
        this.setHasGang(true);
      }
      for (Map.Entry<ReservationInterval, Resource> e : r
          .getAllocationRequests().entrySet()) {
        super.resourcesOverTime.addInterval(e.getKey(), e.getValue());
      }
    }
  }

  @Override
  public Map<String, ReservationAllocation> getPerLabelAllocations() {
    return perLabelAllocations;
  }

  public void setPerLabelAllocations(
      Map<String, ReservationAllocation> perLabelAllocations) {
    this.perLabelAllocations = perLabelAllocations;
  }

  @Override
  public Map<ReservationInterval, Resource> getAllocationRequests() {
    return getAllocationRequests(RMNodeLabelsManager.NO_LABEL);
  }

  @Override
  public Resource getResourcesAtTime(long t, String label) {
    return perLabelAllocations.get(label).getResourcesAtTime(t);
  }

  @Override
  public Map<ReservationInterval, Resource> getAllocationRequests(
      String nodeLabel) {
    if (perLabelAllocations.containsKey(nodeLabel)) {
      return perLabelAllocations.get(nodeLabel)
          .getAllocationRequests(nodeLabel);
    } else {
      return null;
    }
  }

  @Override
  public List<String> getNodeLabels() {
    List<String> list = new ArrayList<String>();
    list.addAll(perLabelAllocations.keySet());
    return Collections.unmodifiableList(list);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, ReservationAllocation> e : perLabelAllocations
        .entrySet()) {
      sb.append(e.getKey() + " = " + e.getValue());
    }
    return sb.toString();
  }
  
  @Override
  public long getStartTime() {
    // note: this has been initialized to the earliest start time of all the per
    // node-label ReservationAllocation
    return startTime;
  }

  @Override
  public long getEndTime() {
    // note: this has been initialized to the latest end time of all the per
    // node-label ReservationAllocation
    return endTime;
  }
  
  @Override
  public long getStartTime(String label) {
    if(perLabelAllocations.containsKey(label)){
      return perLabelAllocations.get(label).getStartTime(label);
    } else {
      return -1;
    }
  }

  @Override
  public long getEndTime(String label) {
    if(perLabelAllocations.containsKey(label)){
      return perLabelAllocations.get(label).getEndTime(label);
    } else {
      return -1;
    }
  }

}
