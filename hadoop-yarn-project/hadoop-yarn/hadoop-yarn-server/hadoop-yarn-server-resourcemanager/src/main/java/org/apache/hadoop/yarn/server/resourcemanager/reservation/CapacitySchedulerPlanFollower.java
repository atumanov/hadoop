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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.PlanQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ReservationQueue;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a {@link PlanFollower}. This is invoked on a timer, and
 * it is in charge to publish the state of the {@link Plan}s to the underlying
 * {@link CapacityScheduler}. This implementation does so, by
 * adding/removing/resizing leaf queues in the scheduler, thus affecting the
 * dynamic behavior of the scheduler in a way that is consistent with the
 * content of the plan. It also updates the plan's view on how much resources
 * are available in the cluster.
 * 
 * This implementation of PlanFollower is relatively stateless, and it can
 * synchronize schedulers and Plans that have arbitrary changes (performing set
 * differences among existing queues). This makes it resilient to frequency of
 * synchronization, and RM restart issues (no "catch up" is necessary).
 */
public class CapacitySchedulerPlanFollower extends AbstractSchedulerPlanFollower {

  private static final Logger LOG = LoggerFactory
      .getLogger(CapacitySchedulerPlanFollower.class);

  private CapacityScheduler cs;

  @Override
  public void init(Clock clock, ResourceScheduler sched, Collection<Plan> plans) {
    super.init(clock, sched, plans);
    LOG.info("Initializing Plan Follower Policy:"
        + this.getClass().getCanonicalName());
    if (!(sched instanceof CapacityScheduler)) {
      throw new YarnRuntimeException(
          "CapacitySchedulerPlanFollower can only work with CapacityScheduler");
    }
    this.cs = (CapacityScheduler) sched;
  }

  @Override
  protected Queue getPlanQueue(String planQueueName) {
    CSQueue queue = cs.getQueue(planQueueName);
    if (!(queue instanceof PlanQueue)) {
      LOG.error("The Plan is not an PlanQueue!");
      return null;
    }
    return queue;
  }

  @Override
  protected float calculateReservationToPlanRatio(
      Resource clusterResources, Resource planResources,
      Resource reservationResources) {
    return Resources.divide(cs.getResourceCalculator(),
        clusterResources, reservationResources, planResources);
  }

  /*
  @Override
  protected boolean arePlanResourcesLessThanReservations(
      Map<String,Resource> clusterResources, 
      Map<String,Resource> planResources,
      Map<String,Resource> reservedResources) {
    
    for (String l: planResources.keySet()) {
      // TODO: handle null Resources (no downstream checks for that)
      boolean nlGreater = Resources.greaterThan(cs.getResourceCalculator(),
          clusterResources.get(l), reservedResources.get(l), planResources.get(l));
      if (!nlGreater)
        return false;
    }
    
    return true;
  }
  */

  @Override
  protected List<? extends Queue> getChildReservationQueues(Queue queue) {
    PlanQueue planQueue = (PlanQueue)queue;
    List<CSQueue> childQueues = planQueue.getChildQueues();
    return childQueues;
  }

  @Override
  protected void addReservationQueue(
      String planQueueName, Queue queue, String currResId) {
    PlanQueue planQueue = (PlanQueue)queue;
    try {
      ReservationQueue resQueue =
          new ReservationQueue(cs, currResId, planQueue);
      cs.addQueue(resQueue);
    } catch (SchedulerDynamicEditException e) {
      LOG.warn(
          "Exception while trying to activate reservation: {} for plan: {}",
          currResId, planQueueName, e);
    } catch (IOException e) {
      LOG.warn(
          "Exception while trying to activate reservation: {} for plan: {}",
          currResId, planQueueName, e);
    }
  }

  @Override
  protected void createDefaultReservationQueue(
      String planQueueName, Queue queue, String defReservationId) {
    PlanQueue planQueue = (PlanQueue)queue;
    if (cs.getQueue(defReservationId) == null) {
      try {
        ReservationQueue defQueue =
            new ReservationQueue(cs, defReservationId, planQueue);
        cs.addQueue(defQueue);
      } catch (SchedulerDynamicEditException e) {
        LOG.warn(
            "Exception while trying to create default reservation queue for plan: {}",
            planQueueName, e);
      } catch (IOException e) {
        LOG.warn(
            "Exception while trying to create default reservation queue for " +
                "plan: {}",
            planQueueName, e);
      }
    }
  }

  // TODO(atumanov): abscap per label (from QueueCapacities) x Resource per label (labelManager)
  /**
   * 
   * @param plan - Reservation plan
   * @param queue - Reservation queue
   * @param nlclusterResources
   * @return Map<String, Resources> -- a set of resources consumed by the plan queue
   * per label
   * side-effect : also propagates absolute queue capacity to the plan
   */
  @Override
  protected Map<String, Resource> getPlanResources(
      Plan plan, Queue queue, List<RMNodeLabel> rmNodeLabelList) {

    PlanQueue planQueue = (PlanQueue)queue;
    QueueCapacities planqcap = planQueue.getQueueCapacities();
    Set<String> planqcaplabels = planqcap.getExistingNodeLabels();
    // create a lookup table mapping label strings to label objects for convenience.
    Map<String, RMNodeLabel> rmNodeLabelMap = new HashMap<String, RMNodeLabel>();
    Map<String, RMNodeLabel> planqNodeLabelMap = new HashMap<String, RMNodeLabel>();
    Map<String, Resource> planqNodeLabelResources = new HashMap<String, Resource>();
    for (RMNodeLabel rmnl: rmNodeLabelList) {
      rmNodeLabelMap.put(rmnl.getLabelName(), rmnl);
    }
    // get absolute queue capacity vector
    // float planAbsCap = planQueue.getAbsoluteCapacity();
    // Iterate over all plan queue labels. For each label, calculate resource.
    // Assumption: planqcaplabels are unique
    for (String l : planqcaplabels) { // for each plan queue label
      // look up its absolute capacity in the queue
      float abscap = planqcap.getAbsoluteCapacity(l);

      RMNodeLabel rmnl = rmNodeLabelMap.get(l);
      if (rmnl == null) {
        // This should not happen
        LOG.error("[getPlanResources] Missing plan queue label " + l + " in the set of cluster labels " + planqcaplabels.toString());
        System.out.println("[getPlanResources] Missing plan queue label " + l + " in the set of cluster labels " + planqcaplabels.toString());
        continue;
      }
      rmnl = rmnl.getCopy();
      Resource labelRes = Resources.multiply(rmnl.getResource(), abscap);
      rmnl.setResource(labelRes); // is this necessary?
      planqNodeLabelMap.put(l, rmnl);
      planqNodeLabelResources.put(l, rmnl.getResource());
    }
    plan.setTotalCapacity(planqNodeLabelMap); // Map<String, RMNodeLabel>
    return planqNodeLabelResources; // Map<String, Resource>
  }

  @Override
  protected Resource getReservationQueueResourceIfExists(Plan plan,
      ReservationId reservationId) {
    CSQueue resQueue = cs.getQueue(reservationId.toString());
    Resource reservationResource = null;
    if (resQueue != null) {
      reservationResource = Resources.multiply(cs.getClusterResource(),
          resQueue.getAbsoluteCapacity());
    }
    return reservationResource;
  }

}
