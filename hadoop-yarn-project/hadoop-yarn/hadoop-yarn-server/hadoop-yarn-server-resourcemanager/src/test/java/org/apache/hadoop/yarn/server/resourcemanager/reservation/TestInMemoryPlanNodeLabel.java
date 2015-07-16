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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.nodelabels.RMNodeLabel;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestInMemoryPlanNodeLabel {

  private String user = "yarn";
  private String planName = "test-reservation";
  private ResourceCalculator resCalc;
  private Resource minAlloc;
  private Resource maxAlloc;
  private Map<String, RMNodeLabel> totalCapacities;

  private Clock clock;
  private QueueMetrics queueMetrics;
  private SharingPolicy policy;
  private ReservationAgent agent;
  private Planner replanner;
  public String label1 = "red";
  public String label2 = "blue";

  @Before
  public void setUp() throws PlanningException {
    resCalc = new DefaultResourceCalculator();
    minAlloc = Resource.newInstance(1024, 1);
    maxAlloc = Resource.newInstance(64 * 1024, 20);

    Resource blueCapacity = Resource.newInstance(50 * 1024, 50);
    Resource redCapacity = Resource.newInstance(50 * 1024, 50);

    totalCapacities = new HashMap<String, RMNodeLabel>();
    totalCapacities.put(RMNodeLabelsManager.NO_LABEL, new RMNodeLabel(
        RMNodeLabelsManager.NO_LABEL, Resource.newInstance(200 * 1024, 200),
        -1, false));
    totalCapacities.put(label1, new RMNodeLabel(label1, blueCapacity, -1, false));
    totalCapacities.put(label2, new RMNodeLabel(label2, redCapacity, -1, false));

    clock = mock(Clock.class);
    queueMetrics = mock(QueueMetrics.class);
    policy = mock(SharingPolicy.class);
    replanner = mock(Planner.class);

    when(clock.getTime()).thenReturn(1L);
  }

  @After
  public void tearDown() {
    resCalc = null;
    minAlloc = null;
    maxAlloc = null;
    totalCapacities = null;

    clock = null;
    queueMetrics = null;
    policy = null;
    replanner = null;
  }

  @Test
  public void testAddReservation() {
    Plan plan = new InMemoryPlan(queueMetrics, policy, agent, totalCapacities,
        1L, resCalc, minAlloc, maxAlloc, planName, replanner, true,
        new UTCClock());
    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();

    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    Map<ReservationInterval, ReservationRequest> allocations = generateAllocation(
        start, alloc, false);
    ReservationDefinition rDef = createSimpleReservationDefinition(start, start
        + alloc.length, alloc.length, allocations.values());
    ReservationAllocation rAllocation = new InMemoryReservationAllocation(
        reservationID, rDef, user, planName, start, start + alloc.length,
        allocations, resCalc, minAlloc, label1);

    int[] alloc2 = { 20, 20, 20};
    int start2 = 103;
    Map<ReservationInterval, ReservationRequest> allocations2 = generateAllocation(
        start2, alloc2, false);
    ReservationDefinition rDef2 = createSimpleReservationDefinition(start, start
        + alloc.length, alloc.length, allocations.values());
    ReservationAllocation rAllocation2 = new InMemoryReservationAllocation(
        reservationID, rDef2, user, planName, start2, start2 + alloc2.length,
        allocations2, resCalc, minAlloc, label2);

    Map<String, ReservationAllocation> resAllocations = new HashMap<>();
    resAllocations.put(rAllocation.getNodeLabels().get(0), rAllocation);
    resAllocations.put(rAllocation2.getNodeLabels().get(0), rAllocation2);

    MultiNodeLabelReservationAllocation mnlResAlloc = new MultiNodeLabelReservationAllocation(
        reservationID, rDef, user, planName, resAllocations, resCalc, minAlloc);

    Assert.assertNull(plan.getReservationById(reservationID));
    try {
      plan.addReservation(mnlResAlloc);
    } catch (PlanningException e) {
      Assert.fail(e.getMessage());
    }
    doAssertions(plan, mnlResAlloc);
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(Resource.newInstance(1024 * (alloc[i]), (alloc[i])),
          plan.getTotalCommittedResources(start + i,label1));
    }
    
    for (int i = 0; i < alloc2.length; i++) {
      Assert.assertEquals(Resource.newInstance(1024 * (alloc2[i]), (alloc2[i])),
          plan.getTotalCommittedResources(start2 + i,label2));
    }
    
    for (int i = start; i < start + alloc2.length; i++) {
      
      if(i < start2){
        Assert.assertEquals(Resource.newInstance(1024 * (alloc[i-start]), (alloc[i-start])),
            plan.getTotalCommittedResources(i,label1 + " || " + label2));
        Assert.assertEquals(Resource.newInstance(1024 * (alloc[i-start]), (alloc[i-start])),
            plan.getConsumptionForUser(user,i));
      }
      if(i > start2 && i < start + alloc.length) {
        Assert.assertEquals(Resource.newInstance(1024 * (alloc[i-start] + alloc[i-start2]), (alloc[i-start] + alloc[i-start2])),
            plan.getTotalCommittedResources(i,label1 + " || " + label2));
        Assert.assertEquals(Resource.newInstance(1024 * (alloc[i-start] + alloc[i-start2]), (alloc[i-start] + alloc[i-start2])),
            plan.getConsumptionForUser(user, i));
      }
      if(i > start + alloc.length){
        Assert.assertEquals(Resource.newInstance(1024 * (alloc2[i-start2]), (alloc2[i-start2])),
            plan.getTotalCommittedResources(i,label1 + " || " + label2));
        Assert.assertEquals(Resource.newInstance(1024 * (alloc2[i]), (alloc2[i])),
            plan.getConsumptionForUser(user, i));
      }
      
      

    }
    
  }

  private void doAssertions(Plan plan, ReservationAllocation rAllocation) {
    ReservationId reservationID = rAllocation.getReservationId();
    Assert.assertNotNull(plan.getReservationById(reservationID));
    Assert.assertEquals(rAllocation, plan.getReservationById(reservationID));
    Assert.assertTrue(((InMemoryPlan) plan).getAllReservations().size() == 1);
    long allocTime = rAllocation.getEndTime(label1);
    long planTime = plan.getLastEndTime(label1);
    Assert.assertEquals(allocTime, planTime);
    long allocTime2 = rAllocation.getEndTime(label2);
    long planTime2 = plan.getLastEndTime(label2);
    Assert.assertEquals(allocTime2, planTime2);
    Assert.assertEquals(totalCapacities.get(RMNodeLabelsManager.NO_LABEL)
        .getResource(), plan.getTotalCapacity());
    Assert.assertEquals(minAlloc, plan.getMinimumAllocation());
    Assert.assertEquals(maxAlloc, plan.getMaximumAllocation());
    Assert.assertEquals(resCalc, plan.getResourceCalculator());
    Assert.assertEquals(planName, plan.getQueueName());
    Assert.assertTrue(plan.getMoveOnExpiry());
  }

  private ReservationDefinition createSimpleReservationDefinition(long arrival,
      long deadline, long duration, Collection<ReservationRequest> resources) {
    // create a request with a single atomic ask
    ReservationDefinition rDef = new ReservationDefinitionPBImpl();
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(new ArrayList<ReservationRequest>(resources));
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    rDef.setReservationRequests(reqs);
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    return rDef;
  }

  private Map<ReservationInterval, ReservationRequest> generateAllocation(
      int startTime, int[] alloc, boolean isStep) {
    Map<ReservationInterval, ReservationRequest> req = new HashMap<ReservationInterval, ReservationRequest>();
    int numContainers = 0;
    for (int i = 0; i < alloc.length; i++) {
      if (isStep) {
        numContainers = alloc[i] + i;
      } else {
        numContainers = alloc[i];
      }
      ReservationRequest rr = ReservationRequest.newInstance(
          Resource.newInstance(1024, 1), (numContainers));
      req.put(new ReservationInterval(startTime + i, startTime + i + 1), rr);
    }
    return req;
  }

}
