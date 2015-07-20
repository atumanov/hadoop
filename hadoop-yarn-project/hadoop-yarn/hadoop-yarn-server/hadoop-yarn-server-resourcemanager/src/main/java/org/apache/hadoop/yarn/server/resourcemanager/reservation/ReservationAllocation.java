/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * A ReservationAllocation represents a concrete allocation of resources over
 * time that satisfy a certain {@link ReservationDefinition}. This is used
 * internally by a {@link Plan} to store information about how each of the
 * accepted {@link ReservationDefinition} have been allocated.
 */
public interface ReservationAllocation extends
    Comparable<ReservationAllocation> {

  /**
   * Returns the unique identifier {@link ReservationId} that represents the
   * reservation
   * 
   * @return reservationId the unique identifier {@link ReservationId} that
   *         represents the reservation
   */
  public ReservationId getReservationId();

  /**
   * Returns the original {@link ReservationDefinition} submitted by the client
   * 
   * @return the {@link ReservationDefinition} submitted by the client
   */
  public ReservationDefinition getReservationDefinition();

  /**
   * Returns the time at which the reservation is activated
   * 
   * @return the time at which the reservation is activated
   */
  public long getStartTime();

  /**
   * Returns the time at which the reservation terminates
   * 
   * @return the time at which the reservation terminates
   */
  public long getEndTime();
  
  /**
   * Returns the time at which the reservation is activated for this label
   * 
   * @param label
   * @return the time at which the reservation is activated
   */
  public long getStartTime(String label);

  /**
   * Returns the time at which the reservation terminates for this label
   * 
   * @param label
   * @return the time at which the reservation terminates
   */
  public long getEndTime(String label);
  

  /**
   * Set the time at which the reservation is activated
   * 
   * @param the time at which the reservation is activated
   */

  public void setStartTime(long startTime);

  /**
   * Set the time at which the reservation terminates
   * 
   * @param the time at which the reservation terminates
   */

  public void setEndTime(long endTime);

  /**
   * Returns the map of resources requested against the time interval for which
   * they were allocated (for NO_LABEL resources).
   * 
   * @return the allocationRequests the map of resources requested against the
   *         time interval for which they were
   */
  public Map<ReservationInterval, Resource> getAllocationRequests();
  
  
  /**
   * Returns the map of resources requested against the time interval for which
   * they were allocated for a given nodeLabel
   * 
   * @return the allocationRequests the map of resources requested against the
   *         time interval for which they were
   */
  public Map<ReservationInterval, Resource> getAllocationRequests(String nodeLabel);

  /**
   * Return the list of all node labels this reservation is allocating against
   * 
   * @return list of node labels
   */
  public List<String> getNodeLabels();
  
  /**
   * Return a string identifying the plan to which the reservation belongs
   * 
   * @return the plan to which the reservation belongs
   */
  public String getPlanName();

  /**
   * Returns the user who requested the reservation
   * 
   * @return the user who requested the reservation
   */
  public String getUser();

  /**
   * Returns whether the reservation has gang semantics or not
   * 
   * @return true if there is a gang request, false otherwise
   */
  public boolean containsGangs();

  /**
   * Set whether any of the stages in this reservation has gang semantics
   * @param hasGang
   */
  public void setHasGang(boolean hasGang);

  /**
   * Sets the time at which the reservation was accepted by the system
   * 
   * @param acceptedAt the time at which the reservation was accepted by the
   *          system
   */
  public void setAcceptanceTimestamp(long acceptedAt);

  /**
   * Returns the time at which the reservation was accepted by the system
   * 
   * @return the time at which the reservation was accepted by the system
   */
  public long getAcceptanceTime();

  /**
   * Returns the capacity represented by cumulative resources reserved by the
   * reservation at the specified point of time
   * 
   * @param tick the time (UTC in ms) for which the reserved resources are
   *          requested
   * @return the resources reserved at the specified time
   */
  public Resource getResourcesAtTime(long tick);

  /**
   * Returns the capacity represented by cumulative resources reserved by the
   * reservation at the specified point of time on a specific node label
   * 
   * @param tick the time (UTC in ms) for which the reserved resources are
   *          requested
   * @param label the node label we are referring to         
   * @return the resources reserved at the specified time/label
   */  
  public Resource getResourcesAtTime(long t, String label);

  /**
   * This method return a map between node-labels and ReservationAllocation 
   * (capturing the resources allocated for each of the node-label)
   * 
   * @return a map of node-labels to reservation allocations 
   */
  Map<String, ReservationAllocation> getPerLabelAllocations();
  

  

}
