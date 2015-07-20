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
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestMultiNodeLabelReservationAllocation {

  private String user = "yarn";
  private String planName = "test-reservation";
  private ResourceCalculator resCalc;
  private Resource minAlloc;
  private String label1 = "red";
  private String label2 = "blue";

  private Random rand = new Random();

  @Before
  public void setUp() {
    resCalc = new DefaultResourceCalculator();
    minAlloc = Resource.newInstance(1, 1);
  }

  @After
  public void tearDown() {
    user = null;
    planName = null;
    resCalc = null;
    minAlloc = null;
  }

  @Test
  public void testBlocks() {
    ReservationId reservationID = ReservationId.newInstance(rand.nextLong(),
        rand.nextLong());

    // generate allocation for label1
    int[] alloc = { 10, 10, 10, 10, 10, 10 };
    int start = 100;
    ReservationDefinition rDef = createSimpleReservationDefinition(start, start
        + alloc.length + 1, alloc.length);
    Map<ReservationInterval, Resource> allocations = generateAllocation(
        start, alloc, false, false);
    ReservationAllocation rAllocation = new InMemoryReservationAllocation(
        reservationID, rDef, user, planName, start, start + alloc.length + 1,
        allocations, resCalc, minAlloc, label1);

    // generate different allocation for label2
    int[] alloc2 = { 20, 20, 20 };
    int start2 = 200;
    Map<ReservationInterval, Resource> allocations2 = generateAllocation(
        start2, alloc2, false, false);
    ReservationAllocation rAllocation2 = new InMemoryReservationAllocation(
        reservationID, rDef, user, planName, start2,
        start2 + alloc2.length + 1, allocations2, resCalc, minAlloc, label2);

    Map<String, ReservationAllocation> resAllocations = new HashMap<>();
    resAllocations.put(rAllocation.getNodeLabels().get(0), rAllocation);
    resAllocations.put(rAllocation2.getNodeLabels().get(0), rAllocation2);

    MultiNodeLabelReservationAllocation mnlResAlloc = new MultiNodeLabelReservationAllocation(
        reservationID, rDef, user, planName, resAllocations, resCalc, minAlloc);

    System.out.println(mnlResAlloc);

    doAssertions(mnlResAlloc, reservationID, rDef, allocations, start, alloc,
        allocations2, start2, alloc2);
    Assert.assertFalse(rAllocation.containsGangs());
    for (int i = 0; i < alloc.length; i++) {
      Assert.assertEquals(Resource.newInstance(1024 * (alloc[i]), (alloc[i])),
          rAllocation.getResourcesAtTime(start + i));
    }
  }

  private void doAssertions(ReservationAllocation rAllocation,
      ReservationId reservationID, ReservationDefinition rDef,
      Map<ReservationInterval, Resource> allocations, int start,
      int[] alloc, Map<ReservationInterval, Resource> allocations2,
      int start2, int[] alloc2) {
    Assert.assertEquals(reservationID, rAllocation.getReservationId());
    Assert.assertEquals(rDef, rAllocation.getReservationDefinition());
    Assert.assertEquals(allocations, rAllocation.getAllocationRequests(label1));
    Assert
        .assertEquals(allocations2, rAllocation.getAllocationRequests(label2));
    Assert.assertEquals(user, rAllocation.getUser());
    Assert.assertEquals(planName, rAllocation.getPlanName());
    Assert.assertEquals(start, rAllocation.getStartTime());
    Assert.assertEquals(start2 + alloc2.length + 1, rAllocation.getEndTime());
  }

  private ReservationDefinition createSimpleReservationDefinition(long arrival,
      long deadline, long duration) {
    // create a request with a single atomic ask
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 1, 1, duration);
    ReservationDefinition rDef = new ReservationDefinitionPBImpl();
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(Collections.singletonList(r));
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    rDef.setReservationRequests(reqs);
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    return rDef;
  }

  private Map<ReservationInterval, Resource> generateAllocation(
      int startTime, int[] alloc, boolean isStep, boolean isGang) {
    Map<ReservationInterval, Resource> req = new HashMap<ReservationInterval, Resource>();
    int numContainers = 0;
    for (int i = 0; i < alloc.length; i++) {
      if (isStep) {
        numContainers = alloc[i] + i;
      } else {
        numContainers = alloc[i];
      }
      Resource rr = Resource.newInstance(1024 * numContainers, numContainers);
      req.put(new ReservationInterval(startTime + i, startTime + i + 1), rr);
    }
    return req;
  }

}
