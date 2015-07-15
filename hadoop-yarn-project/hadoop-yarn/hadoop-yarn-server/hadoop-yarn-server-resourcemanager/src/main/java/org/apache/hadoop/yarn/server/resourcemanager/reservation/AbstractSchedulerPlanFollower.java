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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractSchedulerPlanFollower implements PlanFollower {
  private static final Logger LOG = LoggerFactory
      .getLogger(CapacitySchedulerPlanFollower.class);

  protected Collection<Plan> plans = new ArrayList<Plan>();
  protected YarnScheduler scheduler;
  protected Clock clock;

  @Override
  public void init(Clock clock, ResourceScheduler sched, Collection<Plan> plans) {
    this.clock = clock;
    this.scheduler = sched;
    this.plans.addAll(plans);
  }

  @Override
  public synchronized void run() {
    for (Plan plan : plans) {
      synchronizePlan(plan);
    }
  }

  @Override
  public synchronized void setPlans(Collection<Plan> plans) {
    this.plans.clear();
    this.plans.addAll(plans);
  }

  @Override
  public synchronized void synchronizePlan(Plan plan) {
     String planQueueName = plan.getQueueName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running plan follower edit policy for plan: " + planQueueName);
    }
    // align with plan step
    long step = plan.getStep();
    long now = clock.getTime();
    if (now % step != 0) {
      now += step - (now % step);
    }
    Queue planQueue = getPlanQueue(planQueueName);
    if (planQueue == null) return;

    // first we publish to the plan the current availability of resources
    // TODO(atumanov): cluster resource availability should be changed to be per label
    // expect : Map<String, Resource> clusterResourceMap = scheduler.getClusterResource();
    
    // construct cluster resources by node label
    //Resource clusterResources = scheduler.getClusterResource();
    List<RMNodeLabel> rmNodeLabelList = scheduler.getRMContext()
        .getNodeLabelManager().pullRMNodeLabelsInfo();
    Map<String, Resource> nlClusterResources = new HashMap<String, Resource>();
    Map<String, Resource> reservedResources = new HashMap<String, Resource>();
    for (RMNodeLabel rmnl: rmNodeLabelList) {
      nlClusterResources.put(rmnl.getLabelName(), rmnl.getResource());
      reservedResources.put(rmnl.getLabelName(), Resource.newInstance(0,0));
    }
    // get the amount of resources used by the current plan
    
    Map<String, Resource> planResources = getPlanResources(plan, planQueue, rmNodeLabelList);

    Set<ReservationAllocation> currentReservations =
        plan.getReservationsAtTime(now);
    
    // TODO(atumanov): ReservationAllocations returned in a map
    // expect: Map<String, Set<ReservationAllocation>> curResMap
    // for now : construct this map after the call
    Map<String, Set<ReservationAllocation>> curResMap = new HashMap<String, Set<ReservationAllocation>>();
    curResMap.put(CommonNodeLabelsManager.NO_LABEL, currentReservations);
    Set<String> curReservationNames = new HashSet<String>();
    //Resource reservedResources = Resource.newInstance(0, 0);
    // curReservationNames is populated with names of current reservations as a side-effect 
    int numRes = getReservedResources(now, currentReservations,
        curReservationNames, reservedResources);

    // create the default reservation queue if it doesn't exist
    String defReservationId = getReservationIdFromQueueName(planQueueName) +
        ReservationConstants.DEFAULT_QUEUE_SUFFIX;
    String defReservationQueue = getReservationQueueName(planQueueName,
        defReservationId);
    createDefaultReservationQueue(planQueueName, planQueue,
        defReservationId);
    curReservationNames.add(defReservationId);

    // if the resources dedicated to this plan shrunk, invoke replanner
    // TODO: arePlanResourcesLessThanReservations --> change to do this per partition
    // clusterResources and planResources must both be per partition
    if (arePlanResourcesLessThanReservations(nlClusterResources, planResources,
        reservedResources)) {
      try {
        plan.getReplanner().plan(plan, null);
        // TODO(atumanov) : duplicate the code querying the plan data structs, 
        // reconstruct curReservationNames
      } catch (PlanningException e) {
        LOG.warn("Exception while trying to replan: {}", planQueueName, e);
      }
    }

    // identify the reservations that have expired and new reservations that
    // have to be activated
    List<? extends Queue> resQueues = getChildReservationQueues(planQueue);
    Set<String> expired = new HashSet<String>();
    for (Queue resQueue : resQueues) {
      String resQueueName = resQueue.getQueueName();
      String reservationId = getReservationIdFromQueueName(resQueueName);
      if (curReservationNames.contains(reservationId)) {
        // it is already existing reservation, so needed not create new
        // reservation queue
        curReservationNames.remove(reservationId); // already exists
      } else {
        // the reservation has termination, mark for cleanup
        expired.add(reservationId); // no longer exists
      }
    }
    // garbage collect expired reservations
    cleanupExpiredQueues(planQueueName, plan.getMoveOnExpiry(), expired,
        defReservationQueue);

    // extract all the unique labels from currentReservations
    // external for loop over those labels
    // per each label do:
    //     get all reservations using this label
    //     sort them by delta
    //     for each queue in sorted queue list:
    //         setEntitlement(q)
    
    // Add new reservations and update existing ones
    float totalAssignedCapacity = 0f;
    if (currentReservations != null) {
      // first release all excess capacity in default queue
      try {
    	QueueCapacities newcap = new QueueCapacities(false);
    	newcap.setCapacity(0f);
    	newcap.setMaximumCapacity(1.0f);
        setQueueEntitlement(planQueueName, defReservationQueue, newcap);
      } catch (YarnException e) {
        LOG.warn(
            "Exception while trying to release default queue capacity for plan: {}",
            planQueueName, e);
      }
      // sort allocations from the one giving up the most resources, to the
      // one asking for the most
      // avoid order-of-operation errors that temporarily violate 100%
      // capacity bound
      List<ReservationAllocation> sortedAllocations =
          sortByDelta(
              new ArrayList<ReservationAllocation>(currentReservations), now,
              plan);
      for (ReservationAllocation res : sortedAllocations) {
        String currResId = res.getReservationId().toString();
        if (curReservationNames.contains(currResId)) {
          // TODO: why is this check needed? curReservationNames is derived from currentReservations
          // NOTE: queue should only be added once. Capacity assignment -- per label
          // possibility: curReservationNames.remove(currResId); 
          addReservationQueue(planQueueName, planQueue, currResId);
        }
        Resource capToAssign = res.getResourcesAtTime(now); // query MultiNLReservationAllocation.getResourcesAtTime(now, l);
        float targetCapacity = 0f;
        // if the curLabel slice of planResources is positive, calculate targetCap and setEntitlement
        if (planResources.getMemory() > 0
            && planResources.getVirtualCores() > 0) {
          targetCapacity =
              calculateReservationToPlanRatio(clusterResources,
                  planResources,
                  capToAssign);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Assigning capacity of {} to queue {} with target capacity {}",
              capToAssign, currResId, targetCapacity);
        }
        // set maxCapacity to 100% unless the job requires gang, in which
        // case we stick to capacity (as running early/before is likely a
        // waste of resources)
        float maxCapacity = 1.0f;
        if (res.containsGangs()) {
          maxCapacity = targetCapacity;
        }
        try {
          QueueCapacities newcap = new QueueCapacities(false);
          newcap.setCapacity(targetCapacity);
          newcap.setMaximumCapacity(maxCapacity);
          setQueueEntitlement(planQueueName, currResId, newcap);
        } catch (YarnException e) {
          LOG.warn("Exception while trying to size reservation for plan: {}",
              currResId, planQueueName, e);
        }
        totalAssignedCapacity += targetCapacity;
      }
    }
    // compute the default queue capacity
    // TODO: this should also be per label, both totalAssignedCapacity and defQCap
    float defQCap = 1.0f - totalAssignedCapacity;
    if (LOG.isDebugEnabled()) {
      LOG.debug("PlanFollowerEditPolicyTask: total Plan Capacity: {} "
          + "currReservation: {} default-queue capacity: {}", planResources,
          numRes, defQCap);
    }
    
    try {
      // set the default queue to absorb all remaining unallocated capacity
      QueueCapacities newcap = new QueueCapacities(false);
      newcap.setCapacity(defQCap);
      newcap.setMaximumCapacity(1.0f);
      // TODO : replace newcap with label-vectorized defQCap 
      setQueueEntitlement(planQueueName, defReservationQueue, newcap);
    } catch (YarnException e) {
      LOG.warn(
          "Exception while trying to reclaim default queue capacity for plan: {}",
          planQueueName, e);
    }
    // garbage collect finished reservations from plan
    try {
      plan.archiveCompletedReservations(now);
    } catch (PlanningException e) {
      LOG.error("Exception in archiving completed reservations: ", e);
    }
    LOG.info("Finished iteration of plan follower edit policy for plan: "
        + planQueueName);

    // Extension: update plan with app states,
    // useful to support smart replanning
  }

  protected String getReservationIdFromQueueName(String resQueueName) {
    return resQueueName;
  }

  protected void setQueueEntitlement(String planQueueName, String currResId,
		  QueueCapacities qcap) throws YarnException {
    String reservationQueueName = getReservationQueueName(planQueueName,
        currResId);
    scheduler.setEntitlement(reservationQueueName, qcap);
  }

  // Schedulers have different ways of naming queues. See YARN-2773
  protected String getReservationQueueName(String planQueueName,
      String reservationId) {
    return reservationId;
  }

  /**
   * First sets entitlement of queues to zero to prevent new app submission.
   * Then move all apps in the set of queues to the parent plan queue's default
   * reservation queue if move is enabled. Finally cleanups the queue by killing
   * any apps (if move is disabled or move failed) and removing the queue
   */
  protected void cleanupExpiredQueues(String planQueueName,
      boolean shouldMove, Set<String> toRemove, String defReservationQueue) {
    for (String expiredReservationId : toRemove) {
      try {
        // reduce entitlement to 0
        String expiredReservation = getReservationQueueName(planQueueName,
            expiredReservationId);
        QueueCapacities newcap = new QueueCapacities(false);
        newcap.setCapacity(0.0f);
        newcap.setMaximumCapacity(0.0f);
        setQueueEntitlement(planQueueName, expiredReservation, newcap);
        if (shouldMove) {
          moveAppsInQueueSync(expiredReservation, defReservationQueue);
        }
        if (scheduler.getAppsInQueue(expiredReservation).size() > 0) {
          scheduler.killAllAppsInQueue(expiredReservation);
          LOG.info("Killing applications in queue: {}", expiredReservation);
        } else {
          scheduler.removeQueue(expiredReservation);
          LOG.info("Queue: " + expiredReservation + " removed");
        }
      } catch (YarnException e) {
        LOG.warn("Exception while trying to expire reservation: {}",
            expiredReservationId, e);
      }
    }
  }

  /**
   * Move all apps in the set of queues to the parent plan queue's default
   * reservation queue in a synchronous fashion
   */
  private void moveAppsInQueueSync(String expiredReservation,
                                   String defReservationQueue) {
    List<ApplicationAttemptId> activeApps =
        scheduler.getAppsInQueue(expiredReservation);
    if (activeApps.isEmpty()) {
      return;
    }
    for (ApplicationAttemptId app : activeApps) {
      // fallback to parent's default queue
      try {
        scheduler.moveApplication(app.getApplicationId(), defReservationQueue);
      } catch (YarnException e) {
        LOG.warn(
            "Encountered unexpected error during migration of application: {}" +
                " from reservation: {}",
            app, expiredReservation, e);
      }
    }
  }

  /**
   * 
   * @param now
   * @param currentReservations -- a set of current reservations
   * @param curReservationNames -- out param, a set of current reservation names
   * @param reservedResources -- out param, aggregate resources used by reservations
   * @return numRes -- reservation count in currentReservations
   */
  protected int getReservedResources(long now, 
      Set<ReservationAllocation> currentReservations, 
      Set<String> curReservationNames,
      Map<String, Resource> reservedResources) {
    int numRes = 0;
    if (currentReservations != null) {
      numRes = currentReservations.size();
      for (ReservationAllocation reservation : currentReservations) {
        curReservationNames.add(reservation.getReservationId().toString());
        Resources.addTo(reservedResources, reservation.getResourcesAtTime(now));
      }
    }
    return numRes;
  }
  
  protected int getReservedResources(long now, Set<ReservationAllocation>
  currentReservations, Set<String> curReservationNames,
                                 Resource reservedResources) {
int numRes = 0;
if (currentReservations != null) {
  numRes = currentReservations.size();
  for (ReservationAllocation reservation : currentReservations) {
    curReservationNames.add(reservation.getReservationId().toString());
    Resources.addTo(reservedResources, reservation.getResourcesAtTime(now));
  }
}
return numRes;
}

  /**
   * Sort in the order from the least new amount of resources asked (likely
   * negative) to the highest. This prevents "order-of-operation" errors related
   * to exceeding 100% capacity temporarily.
   */
  protected List<ReservationAllocation> sortByDelta(
      List<ReservationAllocation> currentReservations, long now, Plan plan) {
    Collections.sort(currentReservations, new ReservationAllocationComparator(
        now, this, plan));
    return currentReservations;
  }

  /**
   * Get queue associated with reservable queue named
   * @param planQueueName Name of the reservable queue
   * @return queue associated with the reservable queue
   */
  protected abstract Queue getPlanQueue(String planQueueName);

  /**
   * Calculates ratio of reservationResources to planResources
   */
  protected abstract float calculateReservationToPlanRatio(
      Resource clusterResources, Resource planResources,
      Resource reservationResources);

  /**
   * Check if plan resources are less than expected reservation resources
   */
  protected abstract boolean arePlanResourcesLessThanReservations(
      Map<String, Resource> clusterResources, 
      Map<String, Resource> planResources,
      Map<String, Resource> reservedResources);

  /**
   * Get a list of reservation queues for this planQueue
   */
  protected abstract List<? extends Queue> getChildReservationQueues(
      Queue planQueue);

  /**
   * Add a new reservation queue for reservation currResId for this planQueue
   */
  protected abstract void addReservationQueue(
      String planQueueName, Queue queue, String currResId);

  /**
   * Creates the default reservation queue for use when no reservation is
   * used for applications submitted to this planQueue
   */
  protected abstract void createDefaultReservationQueue(
      String planQueueName, Queue queue, String defReservationQueue);

  /**
   * Get plan resources for this planQueue
   */
  protected abstract Map<String, Resource> getPlanResources(
      Plan plan, Queue queue, Map<String, Resource> clusterResources);

  /**
   * Get reservation queue resources if it exists otherwise return null
   */
  protected abstract Resource getReservationQueueResourceIfExists(Plan plan,
      ReservationId reservationId);

  private static class ReservationAllocationComparator implements
      Comparator<ReservationAllocation> {
    AbstractSchedulerPlanFollower planFollower;
    long now;
    Plan plan;

    ReservationAllocationComparator(long now,
        AbstractSchedulerPlanFollower planFollower, Plan plan) {
      this.now = now;
      this.planFollower = planFollower;
      this.plan = plan;
    }

    private Resource getUnallocatedReservedResources(
        ReservationAllocation reservation) {
      Resource resResource;
      Resource reservationResource = planFollower
          .getReservationQueueResourceIfExists
              (plan, reservation.getReservationId());
      if (reservationResource != null) {
        resResource =
            Resources.subtract(
                reservation.getResourcesAtTime(now),
                reservationResource);
      } else {
        resResource = reservation.getResourcesAtTime(now);
      }
      return resResource;
    }

    @Override
    public int compare(ReservationAllocation lhs, ReservationAllocation rhs) {
      // compute delta between current and previous reservation, and compare
      // based on that
      Resource lhsRes = getUnallocatedReservedResources(lhs);
      Resource rhsRes = getUnallocatedReservedResources(rhs);
      return lhsRes.compareTo(rhsRes);
    }
  }
}

