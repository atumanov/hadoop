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

import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * An in memory implementation of a reservation allocation using the
 * {@link RLESparseResourceAllocation}
 * 
 */
class MultiNodeLabelReservationAllocation extends InMemoryReservationAllocation {

  private Map<String, ReservationAllocation> perLabelAllocations;

  public MultiNodeLabelReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<String, ReservationAllocation> perLabelAllocations,
      ResourceCalculator calculator, Resource minAlloc, String nodeLabel) {
    super(reservationID, contract, user, planName, startTime, endTime, null,
        calculator, minAlloc);
    this.perLabelAllocations = perLabelAllocations;

    for (ReservationAllocation r : perLabelAllocations.values()) {
      if (r.containsGangs()) {
        this.setHasGang(true);
      }
      for (Map.Entry<ReservationInterval, ReservationRequest> e : r
          .getAllocationRequests().entrySet()) {
        super.resourcesOverTime.addInterval(e.getKey(), e.getValue());
      }
    }
  }

  public Map<String, ReservationAllocation> getPerLabelAllocations() {
    return perLabelAllocations;
  }

  public void setPerLabelAllocations(Map<String, ReservationAllocation> perLabelAllocations) {
    this.perLabelAllocations = perLabelAllocations;
  }

  @Override
  public Map<ReservationInterval, ReservationRequest> getAllocationRequests() {
    return null;
  }
  
  @Override
  public Resource getResourcesAtTime(long t, String label){
    return perLabelAllocations.get(label).getResourcesAtTime(t);
  }
}
