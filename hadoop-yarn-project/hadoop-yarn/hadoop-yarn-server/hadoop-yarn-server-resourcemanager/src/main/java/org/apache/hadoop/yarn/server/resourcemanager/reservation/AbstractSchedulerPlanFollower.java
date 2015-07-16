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

    // construct cluster resources by node label
    // rmNodeLabelList,rmNodeLabelResources -- cluster-level information, NOT queue-level
    List<RMNodeLabel> rmNodeLabelList = scheduler.getRMContext()
        .getNodeLabelManager().pullRMNodeLabelsInfo();
    Map<String, Resource> rmNodeLabelResources = new HashMap<String, Resource>();
    Map<String, Resource> reservedResources = new HashMap<String, Resource>();
    for (RMNodeLabel rmnl: rmNodeLabelList) {
      rmNodeLabelResources.put(rmnl.getLabelName(), rmnl.getResource());
      reservedResources.put(rmnl.getLabelName(), Resource.newInstance(0,0));
    }
    // now get the amount of resources used by the current plan queue
    // qNodeLabelResources -- queue-level information
    Map<String, Resource> qNodeLabelResources = getPlanResources(plan, planQueue, rmNodeLabelList);

    // currentReservations is a collection of label-aware reservations active now.
    // Each such reservation allocation contains skyline accounting per label.
    Set<ReservationAllocation> currentReservations = plan.getReservationsAtTime(now);
    Set<String> curReservationNames = new HashSet<String>();
    Set<String> curReservationLabels = new HashSet<String>();
    //Resource reservedResources = Resource.newInstance(0, 0);
    // curReservationNames is populated with names of current reservations as a side-effect 
    int numRes = getReservedResources(now, currentReservations,
        curReservationNames, reservedResources);
    // post-condition: curReservationNames and reservedResources are set
    if (currentReservations != null) {
      for (ReservationAllocation r : currentReservations) {
        curReservationLabels.addAll(r.getNodeLabels());
      }
    }

    // create the default reservation queue if it doesn't exist
    String defReservationId = getReservationIdFromQueueName(planQueueName) +
        ReservationConstants.DEFAULT_QUEUE_SUFFIX;
    String defReservationQueue = getReservationQueueName(planQueueName,
        defReservationId);
    createDefaultReservationQueue(planQueueName, planQueue,
        defReservationId);
    curReservationNames.add(defReservationId);

    // if the resources dedicated to this plan shrunk, invoke replanner
    if (arePlanResourcesLessThanReservations(rmNodeLabelResources, qNodeLabelResources,
        reservedResources)) {
      try {
        plan.getReplanner().plan(plan, null);
        // TODO : reconstruct curReservationNames after replanning 
      } catch (PlanningException e) {
        LOG.warn("Exception while trying to replan: {}", planQueueName, e);
      }
    }

    // identify the reservations that have expired and new reservations that
    // have to be activated
    List<? extends Queue> resQueues = getChildReservationQueues(planQueue);
    Set<String> expired = new HashSet<String>();
    Set<Queue> resQueuesExpired = new HashSet<Queue>();
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
        resQueuesExpired.add(resQueue);
      }
    }
    // garbage collect expired reservations
    cleanupExpiredQueues(planQueueName, plan.getMoveOnExpiry(), resQueuesExpired,
        defReservationQueue);

    // Algorithm:
    // for each unique label from currentReservations
    //   create a set of reservation allocations for this label
    //   sort this set
    //   for each reservation allocation in the sorted set
    //     calculate targetCapacity
    //     setEntitlement(l, targetCapacity)
    //   end for
    //   set default queue entitlement
    // end for
    
    // Add new reservations and update existing ones
    Map<String, Float> totalAssignedCapacity = new HashMap<String, Float>();
    for (String l : curReservationLabels) {
      totalAssignedCapacity.put(l, 0f);
      // first release all excess capacity in default queue
      try {
    	QueueCapacities defQCapByNodeLabel = new QueueCapacities(false);
    	defQCapByNodeLabel.setCapacity(l, 0f);
    	defQCapByNodeLabel.setMaximumCapacity(l, 1.0f);
    	  // Assumption: capByNodeLabel --> treated downstream as a per-label capacity
        setQueueEntitlement(planQueueName, defReservationQueue, defQCapByNodeLabel);
      } catch (YarnException e) {
        LOG.warn(
            "Exception while trying to release default queue capacity for plan: {}",
            planQueueName, e);
      }
      // construct a set of ReservationAllocations just for the current label
      Set<ReservationAllocation> currentNLReservations = new HashSet<ReservationAllocation>();
      for (ReservationAllocation res: currentReservations) {
        // downcast the reservation to access per label child reservations
        MultiNodeLabelReservationAllocation nlres = (MultiNodeLabelReservationAllocation)res;
        ReservationAllocation curNLRes = nlres.getPerLabelAllocations().get(l);
        if (curNLRes != null) {
          currentNLReservations.add(curNLRes);
        }
      }
      // sort allocations from the one giving up the most resources, to the
      // one asking for the most
      // avoid order-of-operation errors that temporarily violate 100%
      // capacity bound
      List<ReservationAllocation> sortedAllocations =
          sortByDelta(
              new ArrayList<ReservationAllocation>(currentNLReservations), now,
              plan);

      // for each per label child reservation, determine capacity, set entitlement
      for (ReservationAllocation res : sortedAllocations) {
        String currResId = res.getReservationId().toString();
        if (curReservationNames.contains(currResId)) {
          // NOTE: queue should only be added once. Capacity assignment -- per label
          addReservationQueue(planQueueName, planQueue, currResId);
          // remove this reservation from the list of reservations to process
          // Note: this mutates curReservationNames, causing it to drift out of sync with currentReservations
          curReservationNames.remove(currResId);
        }
        Resource capToAssign = res.getResourcesAtTime(now, l);
        
        float targetCapacity = 0f;
        // if the curLabel slice of planResources is positive, calculate targetCap and setEntitlement
        if (qNodeLabelResources.get(l).getMemory() > 0
            && qNodeLabelResources.get(l).getVirtualCores() > 0) {
          targetCapacity =
              calculateReservationToPlanRatio(rmNodeLabelResources.get(l),
                  qNodeLabelResources.get(l),
                  capToAssign);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Assigning capacity of {} to queue {}, label {} with target capacity {}",
              capToAssign, currResId, l, targetCapacity);
        }
        // set maxCapacity to 100% unless the job requires gang, in which
        // case we stick to capacity (as running early/before is likely a
        // waste of resources)
        float maxCapacity = 1.0f;
        if (res.containsGangs()) {
          maxCapacity = targetCapacity;
        }
        // set entitlement on this reservation queue
        try {
          QueueCapacities resQCapByNodeLabel = new QueueCapacities(false);
          resQCapByNodeLabel.setCapacity(l, targetCapacity);
          resQCapByNodeLabel.setMaximumCapacity(l, maxCapacity);
          setQueueEntitlement(planQueueName, currResId, resQCapByNodeLabel);
        } catch (YarnException e) {
          LOG.warn("Exception while trying to size reservation for plan: {}",
              currResId, planQueueName, e);
        }
        // increment totalAssignedCapacity for this label by the targetCapacity assigned to this res
        if (!totalAssignedCapacity.containsKey(l)) {
          totalAssignedCapacity.put(l, 0f);
        }
        totalAssignedCapacity.put(l, totalAssignedCapacity.get(l).floatValue() + targetCapacity);
      } // end for each reservation
    // compute the default queue capacity
    float defQCap = 1.0f - totalAssignedCapacity.get(l);
    if (LOG.isDebugEnabled()) {
      LOG.debug("PlanFollowerEditPolicyTask: total Plan Capacity: {} "
          + "currReservation: {} default-queue capacity: {}", qNodeLabelResources.get(l),
          numRes, defQCap);
    }
    // now set the default queue capacity for curLabel
    try {
      // set the default queue to absorb all remaining unallocated capacity
      QueueCapacities defQCapByNodeLabel = new QueueCapacities(false);
      defQCapByNodeLabel.setCapacity(l, defQCap);
      defQCapByNodeLabel.setMaximumCapacity(l, 1.0f);
      setQueueEntitlement(planQueueName, defReservationQueue, defQCapByNodeLabel);
    } catch (YarnException e) {
      LOG.warn(
          "Exception while trying to reclaim default queue capacity for plan: {}",
          planQueueName, e);
    }
  } // end for each label
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
      boolean shouldMove, Set<Queue> toRemove, String defReservationQueue) {
    // reduce entitlement to 0 for all labels of this queue
    for (Queue q: toRemove) {
      String expiredReservationId = getReservationIdFromQueueName(q.getQueueName());
      // construct an entitlement object encapsulating all labels for this queue
      QueueCapacities zeroqCap = new QueueCapacities(false);
      for (String label : q.getAccessibleNodeLabels()) {
        zeroqCap.setCapacity(label, 0f);
        zeroqCap.setMaximumCapacity(label, 0f);
      }
      
      try {
        // set entitlement generically
        String expiredReservation = getReservationQueueName(planQueueName,
            expiredReservationId);
        setQueueEntitlement(planQueueName, expiredReservation, zeroqCap);
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
      // for each res, for each label, aggregate Resource usage per label
      // NOTE: this works for reservations with different label sets
      for (ReservationAllocation reservation : currentReservations) {
        curReservationNames.add(reservation.getReservationId().toString());
        for (String nl : reservation.getNodeLabels()) {
          if (!reservedResources.containsKey(nl)) {
            // if this label is not yet tracked by our map, add it
            reservedResources.put(nl, Resource.newInstance(0, 0));
          }
          Resources.addTo(reservedResources.get(nl), reservation.getResourcesAtTime(now, nl));
        }
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
  protected boolean arePlanResourcesLessThanReservations(
      Map<String, Resource> clusterResources, 
      Map<String, Resource> planResources,
      Map<String, Resource> reservedResources) {
    for (String l: planResources.keySet()) {
      boolean nlGreater = Resources.greaterThan(scheduler.getResourceCalculator(),
          clusterResources.get(l), reservedResources.get(l), planResources.get(l));
      if (!nlGreater)
        return false;
    }
    return true;
  }

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
      Plan plan, Queue queue, List<RMNodeLabel> rmNodeLabelList);

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

