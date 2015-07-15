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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class InMemoryPlan implements Plan {


  
  // Storing and indexing reservations
  private TreeMap<ReservationInterval, Set<ReservationAllocation>> reservationsByTime =
      new TreeMap<ReservationInterval, Set<ReservationAllocation>>();
  private Map<ReservationId, ReservationAllocation> reservationsById =
      new HashMap<ReservationId, ReservationAllocation>();
  
  // Materialized views on reservation resource utilization
  private RLESparseResourceAllocation globalResourceUtilization;
  private Map<String, RLESparseResourceAllocation> perLabelResourceUtilization;
  private Map<String, RLESparseResourceAllocation> perUserResourceUtilization =
      new HashMap<String, RLESparseResourceAllocation>();

  
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryPlan.class);
  private static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);
  private final static String NOLABEL = RMNodeLabelsManager.NO_LABEL;
  private final ReentrantReadWriteLock readWriteLock =
      new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();
  private final SharingPolicy policy;
  private final ReservationAgent agent;
  private final long step;
  private final ResourceCalculator resCalc;
  private final Resource minAlloc, maxAlloc;
  private final String queueName;
  private final QueueMetrics queueMetrics;
  private final Planner replanner;
  private final boolean getMoveOnExpiry;
  private final Clock clock;

  private Map<String, RMNodeLabel> totalCapacities;

  InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy,
      ReservationAgent agent, Resource totalCapacity, long step,
      ResourceCalculator resCalc, Resource minAlloc, Resource maxAlloc,
      String queueName, Planner replanner, boolean getMoveOnExpiry) {
    this(queueMetrics, policy, agent, totalCapacity, step, resCalc, minAlloc,
        maxAlloc, queueName, replanner, getMoveOnExpiry, new UTCClock());
  }

  InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy,
      ReservationAgent agent, Resource totalCapacity, long step,
      ResourceCalculator resCalc, Resource minAlloc, Resource maxAlloc,
      String queueName, Planner replanner, boolean getMoveOnExpiry, Clock clock) {
    this(queueMetrics, policy, agent, Collections.singletonMap(NOLABEL, new RMNodeLabel(
        NOLABEL, totalCapacity, -1, false)), step, resCalc, 
        minAlloc, maxAlloc, queueName, replanner, getMoveOnExpiry, clock);

  }

  InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy,
      ReservationAgent agent, Map<String, RMNodeLabel> totalCapacities, long step,
      ResourceCalculator resCalc, Resource minAlloc, Resource maxAlloc,
      String queueName, Planner replanner, boolean getMoveOnExpiry, Clock clock) {
    this.queueMetrics = queueMetrics;
    this.policy = policy;
    this.agent = agent;
    this.step = step;
    this.totalCapacities = totalCapacities;
    this.resCalc = resCalc;
    this.minAlloc = minAlloc;
    this.maxAlloc = maxAlloc;
    this.globalResourceUtilization = new RLESparseResourceAllocation(resCalc,
        minAlloc);
    this.perLabelResourceUtilization = Collections.singletonMap(NOLABEL,
        new RLESparseResourceAllocation(resCalc, minAlloc));
    this.queueName = queueName;
    this.replanner = replanner;
    this.getMoveOnExpiry = getMoveOnExpiry;
    this.clock = clock;
  }
  
  @Override
  public QueueMetrics getQueueMetrics() {
    return queueMetrics;
  }

  private void incrementAllocation(ReservationAllocation reservation) {
    assert (readWriteLock.isWriteLockedByCurrentThread());  
    for(String nodeLabel : reservation.getNodeLabels()){
      Map<ReservationInterval, ReservationRequest> allocationRequests =
        reservation.getAllocationRequests(nodeLabel);
      // check if we have encountered the user earlier and if not add an entry
      String user = reservation.getUser();
      RLESparseResourceAllocation resAlloc = perUserResourceUtilization.get(user);
      if (resAlloc == null) {
        resAlloc = new RLESparseResourceAllocation(resCalc, minAlloc);
        perUserResourceUtilization.put(user, resAlloc);
      }
      for (Map.Entry<ReservationInterval, ReservationRequest> r : allocationRequests
          .entrySet()) {
        resAlloc.addInterval(r.getKey(), r.getValue());
        globalResourceUtilization.addInterval(r.getKey(), r.getValue());
        if (!perLabelResourceUtilization.containsKey(nodeLabel)) {
          perLabelResourceUtilization.put(nodeLabel,
              new RLESparseResourceAllocation(resCalc, minAlloc));
        }
        perLabelResourceUtilization.get(nodeLabel).addInterval(r.getKey(),
            r.getValue());
      }
    }
  }

  private void decrementAllocation(ReservationAllocation reservation) {
    assert (readWriteLock.isWriteLockedByCurrentThread());
    
    for(String nodeLabel : reservation.getNodeLabels()){
      Map<ReservationInterval, ReservationRequest> allocationRequests =
          reservation.getAllocationRequests(nodeLabel);
      String user = reservation.getUser();
      RLESparseResourceAllocation resAlloc = perUserResourceUtilization.get(user);
      for (Map.Entry<ReservationInterval, ReservationRequest> r : allocationRequests
          .entrySet()) {
        resAlloc.removeInterval(r.getKey(), r.getValue());
        perLabelResourceUtilization.get(nodeLabel).removeInterval(r.getKey(), r.getValue());
      }
      if (resAlloc.isEmpty()) {
        perUserResourceUtilization.remove(user);
      }
      if(perLabelResourceUtilization.get(nodeLabel).isEmpty()){
        perLabelResourceUtilization.remove(nodeLabel);
      }
    }
  }

  public Set<ReservationAllocation> getAllReservations() {
    readLock.lock();
    try {
      if (reservationsByTime != null) {
        Set<ReservationAllocation> flattenedReservations =
            new HashSet<ReservationAllocation>();
        for (Set<ReservationAllocation> reservationEntries : reservationsByTime
            .values()) {
          flattenedReservations.addAll(reservationEntries);
        }
        return flattenedReservations;
      } else {
        return null;
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean addReservation(ReservationAllocation reservation)
      throws PlanningException {
    // Verify the allocation is memory based otherwise it is not supported
    InMemoryReservationAllocation inMemReservation =
        (InMemoryReservationAllocation) reservation;
    if (inMemReservation.getUser() == null) {
      String errMsg =
          "The specified Reservation with ID "
              + inMemReservation.getReservationId()
              + " is not mapped to any user";
      LOG.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    writeLock.lock();
    try {
      if (reservationsById.containsKey(inMemReservation.getReservationId())) {
        String errMsg =
            "The specified Reservation with ID "
                + inMemReservation.getReservationId() + " already exists";
        LOG.error(errMsg);
        throw new IllegalArgumentException(errMsg);
      }
      // Validate if we can accept this reservation, throws exception if
      // validation fails
      policy.validate(this, inMemReservation);
      // we record here the time in which the allocation has been accepted
      reservation.setAcceptanceTimestamp(clock.getTime());
      ReservationInterval searchInterval =
          new ReservationInterval(inMemReservation.getStartTime(),
              inMemReservation.getEndTime());
      Set<ReservationAllocation> reservations =
          reservationsByTime.get(searchInterval);
      if (reservations == null) {
        reservations = new HashSet<ReservationAllocation>();
      }
      if (!reservations.add(inMemReservation)) {
        LOG.error("Unable to add reservation: {} to plan.",
            inMemReservation.getReservationId());
        return false;
      }
      reservationsByTime.put(searchInterval, reservations);
      reservationsById.put(inMemReservation.getReservationId(),
          inMemReservation);
      incrementAllocation(inMemReservation);
      LOG.info("Sucessfully added reservation: {} to plan.",
          inMemReservation.getReservationId());
      return true;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public boolean updateReservation(ReservationAllocation reservation)
      throws PlanningException {
    writeLock.lock();
    boolean result = false;
    try {
      ReservationId resId = reservation.getReservationId();
      ReservationAllocation currReservation = getReservationById(resId);
      if (currReservation == null) {
        String errMsg =
            "The specified Reservation with ID " + resId
                + " does not exist in the plan";
        LOG.error(errMsg);
        throw new IllegalArgumentException(errMsg);
      }
      // validate if we can accept this reservation, throws exception if
      // validation fails
      policy.validate(this, reservation);
      if (!removeReservation(currReservation)) {
        LOG.error("Unable to replace reservation: {} from plan.",
            reservation.getReservationId());
        return result;
      }
      try {
        result = addReservation(reservation);
      } catch (PlanningException e) {
        LOG.error("Unable to update reservation: {} from plan due to {}.",
            reservation.getReservationId(), e.getMessage());
      }
      if (result) {
        LOG.info("Sucessfully updated reservation: {} in plan.",
            reservation.getReservationId());
        return result;
      } else {
        // rollback delete
        addReservation(currReservation);
        LOG.info("Rollbacked update reservation: {} from plan.",
            reservation.getReservationId());
        return result;
      }
    } finally {
      writeLock.unlock();
    }
  }

  private boolean removeReservation(ReservationAllocation reservation) {
    assert (readWriteLock.isWriteLockedByCurrentThread());
    ReservationInterval searchInterval =
        new ReservationInterval(reservation.getStartTime(),
            reservation.getEndTime());
    Set<ReservationAllocation> reservations =
        reservationsByTime.get(searchInterval);
    if (reservations != null) {
      if (!reservations.remove(reservation)) {
        LOG.error("Unable to remove reservation: {} from plan.",
            reservation.getReservationId());
        return false;
      }
      if (reservations.isEmpty()) {
        reservationsByTime.remove(searchInterval);
      }
    } else {
      String errMsg =
          "The specified Reservation with ID " + reservation.getReservationId()
              + " does not exist in the plan";
      LOG.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    reservationsById.remove(reservation.getReservationId());
    decrementAllocation(reservation);
    LOG.info("Sucessfully deleted reservation: {} in plan.",
        reservation.getReservationId());
    return true;
  }

  @Override
  public boolean deleteReservation(ReservationId reservationID) {
    writeLock.lock();
    try {
      ReservationAllocation reservation = getReservationById(reservationID);
      if (reservation == null) {
        String errMsg =
            "The specified Reservation with ID " + reservationID
                + " does not exist in the plan";
        LOG.error(errMsg);
        throw new IllegalArgumentException(errMsg);
      }
      return removeReservation(reservation);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void archiveCompletedReservations(long tick) {
    // Since we are looking for old reservations, read lock is optimal
    LOG.debug("Running archival at time: {}", tick);
    List<ReservationAllocation> expiredReservations =
        new ArrayList<ReservationAllocation>();
    readLock.lock();
    // archive reservations and delete the ones which are beyond
    // the reservation policy "window"
    try {
      long archivalTime = tick - policy.getValidWindow();
      ReservationInterval searchInterval =
          new ReservationInterval(archivalTime, archivalTime);
      SortedMap<ReservationInterval, Set<ReservationAllocation>> reservations =
          reservationsByTime.headMap(searchInterval, true);
      if (!reservations.isEmpty()) {
        for (Set<ReservationAllocation> reservationEntries : reservations
            .values()) {
          for (ReservationAllocation reservation : reservationEntries) {
            if (reservation.getEndTime() <= archivalTime) {
              expiredReservations.add(reservation);
            }
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    if (expiredReservations.isEmpty()) {
      return;
    }
    // Need write lock only if there are any reservations to be deleted
    writeLock.lock();
    try {
      for (ReservationAllocation expiredReservation : expiredReservations) {
        removeReservation(expiredReservation);
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public Set<ReservationAllocation> getReservationsAtTime(long tick) {
    ReservationInterval searchInterval =
        new ReservationInterval(tick, Long.MAX_VALUE);
    readLock.lock();
    try {
      SortedMap<ReservationInterval, Set<ReservationAllocation>> reservations =
          reservationsByTime.headMap(searchInterval, true);
      if (!reservations.isEmpty()) {
        Set<ReservationAllocation> flattenedReservations =
            new HashSet<ReservationAllocation>();
        for (Set<ReservationAllocation> reservationEntries : reservations
            .values()) {
          for (ReservationAllocation reservation : reservationEntries) {
            if (reservation.getEndTime() > tick) {
              flattenedReservations.add(reservation);
            }
          }
        }
        return Collections.unmodifiableSet(flattenedReservations);
      } else {
        return Collections.emptySet();
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getStep() {
    return step;
  }

  @Override
  public SharingPolicy getSharingPolicy() {
    return policy;
  }

  @Override
  public ReservationAgent getReservationAgent() {
    return agent;
  }

  @Override
  public Resource getConsumptionForUser(String user, long t) {
    readLock.lock();
    try {
      RLESparseResourceAllocation userResAlloc = perUserResourceUtilization.get(user);
      if (userResAlloc != null) {
        return userResAlloc.getCapacityAtTime(t);
      } else {
        return Resources.clone(ZERO_RESOURCE);
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Resource getTotalCommittedResources(long t) {
    readLock.lock();
    try {
      return perLabelResourceUtilization.get(NOLABEL).getCapacityAtTime(t);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ReservationAllocation getReservationById(ReservationId reservationID) {
    if (reservationID == null) {
      return null;
    }
    readLock.lock();
    try {
      return reservationsById.get(reservationID);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Resource getTotalCapacity() {
    return getTotalCapacity(RMNodeLabelsManager.NO_LABEL);
  }
  
  @Override
  public Resource getTotalCapacity(String nodeLabel){
    readLock.lock();
    try {
      // NOTE: we are interpreting this as the total capacity wihout node labels
      if (totalCapacities.containsKey(nodeLabel)) {
        return Resources.clone(totalCapacities
            .get(nodeLabel).getResource());
      } else {
        return Resource.newInstance(0, 0);
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Resource getMinimumAllocation() {
    return Resources.clone(minAlloc);
  }

  @Override
  public void setTotalCapacity(Resource cap) {
    writeLock.lock();
    try {
      if (totalCapacities.containsKey(RMNodeLabelsManager.NO_LABEL)) {
        totalCapacities.get(RMNodeLabelsManager.NO_LABEL).setResource(
            Resources.clone(cap));
      }
    } finally {
      writeLock.unlock();
    }
  }
  
  @Override
  public void setTotalCapacity(Map<String, RMNodeLabel> capacities) {
    writeLock.lock();
    try {
      totalCapacities = capacities;
    } finally {
      writeLock.unlock();
    }
  }

  public long getEarliestStartTime() {
    readLock.lock();
    try {
      return perLabelResourceUtilization.get(NOLABEL).getEarliestStartTime();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public long getLastEndTime() {
    readLock.lock();
    try {
      return perLabelResourceUtilization.get(NOLABEL).getLatestEndTime();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return resCalc;
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public Resource getMaximumAllocation() {
    return Resources.clone(maxAlloc);
  }

  public String toCumulativeString() {
    readLock.lock();
    try {
      return perLabelResourceUtilization.toString();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Planner getReplanner() {
    return replanner;
  }

  @Override
  public boolean getMoveOnExpiry() {
    return getMoveOnExpiry;
  }

  @Override
  public String toString() {
    readLock.lock();
    try {
      StringBuffer planStr = new StringBuffer("In-memory Plan: ");
      planStr.append("Parent Queue: ").append(queueName)
          .append("Total Capacity: ").append(getTotalCapacity()).append("Step: ")
          .append(step);
      for (ReservationAllocation reservation : getAllReservations()) {
        planStr.append(reservation);
      }
      return planStr.toString();
    } finally {
      readLock.unlock();
    }
  }


  
}
