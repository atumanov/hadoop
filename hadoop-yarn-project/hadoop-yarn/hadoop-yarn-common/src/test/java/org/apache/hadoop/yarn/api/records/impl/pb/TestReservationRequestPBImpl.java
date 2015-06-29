package org.apache.hadoop.yarn.api.records.impl.pb;

import org.junit.Assert;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationRequestProto;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestReservationRequestPBImpl {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testReservationRequest() throws Exception {
		final String simple_nl = "x";
		final String simple_nle = "sc1 || sc2";
		
		// initialize an instance of the class that represents the ReservationRequest proto
		ReservationRequestPBImpl orig = new ReservationRequestPBImpl();
		orig.setNodeLabelExpression(simple_nl);
		orig.setConcurrency(10);
		orig.setDuration(100);
		orig.setNumContainers(1000);
		// get the serialized protobuf
		ReservationRequestProto proto = orig.getProto();
		// now attempt to deserialize this protobuf by instantiating a new object from it
		ReservationRequestPBImpl deser = new ReservationRequestPBImpl(proto);
		
		System.out.println("resreq from orig record: " + orig.toString());
		System.out.println("resreq from orig proto: " + proto.toString());
		System.out.println("resreq from deser record: " + deser.toString());

		Assert.assertEquals(orig, deser);
		Assert.assertTrue(orig.equals(deser));
		Assert.assertEquals(orig.getNodeLabelExpression(), deser.getNodeLabelExpression());
		Assert.assertEquals(orig.getNodeLabelExpression(), simple_nl);
		
		// now modify deser and test
		deser.setNodeLabelExpression(simple_nle);
		Assert.assertEquals(simple_nle, deser.getNodeLabelExpression());
		Assert.assertNotEquals(orig.getNodeLabelExpression(), deser.getNodeLabelExpression());
		Assert.assertFalse("deser nle should be different from orig nle", deser.equals(orig));
	}
}
