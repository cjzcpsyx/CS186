package simpledb;

import org.junit.Assert;
import org.junit.Test;
import simpledb.Predicate.Op;

public class IntStatisticsTest {

	/**
	 * Test with a minimum and a maximum that are both negative numbers.
	 */
	@Test public void negativeRangeTest() {
		IntStatistics h = new IntStatistics(10);
		
		// All of the values here are negative.
		// Also, there are more of them than there are bins.
		for (int c = -60; c <= -10; c++) {
			h.addValue(c);
			h.estimateSelectivity(Op.EQUALS, c);
		}


		Assert.assertEquals(0.02, h.estimateSelectivity(Op.EQUALS, -33), 0.01);
	}
	
	/**
	 * Make sure that equality binning does something reasonable.
	 */
	@Test public void opEqualsTest() {
		IntStatistics h = new IntStatistics(10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		
		// This really should return "1.0"; but,
		// be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, 3) > 0.9);
		Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, 8) < 0.001);
	}
	
	/**
	 * Make sure that GREATER_THAN binning does something reasonable.
	 */
	@Test public void opGreaterThanTest() {
		IntStatistics h = new IntStatistics(10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		h.addValue(1);
		h.addValue(10);
		
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, -1) > 0.999);
		Assert.assertEquals(.9, h.estimateSelectivity(Op.GREATER_THAN, 2), .05);
		Assert.assertEquals(.7,h.estimateSelectivity(Op.GREATER_THAN, 4), .05);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, 12) < 0.001);
	}
	
	/**
	 * Make sure that LESS_THAN binning does something reasonable.
	 */
	@Test public void opLessThanTest() {
		IntStatistics h = new IntStatistics(10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		h.addValue(1);
		h.addValue(10);
		
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, -1) < 0.001);
		Assert.assertEquals(.1, h.estimateSelectivity(Op.LESS_THAN, 2), .1);
		Assert.assertEquals(.3, h.estimateSelectivity(Op.LESS_THAN, 4), .1);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, 12) > 0.999);
	}
	
	/**
	 * Make sure that GREATER_THAN_OR_EQ binning does something reasonable.
	 */
	@Test public void opGreaterThanOrEqualsTest() {
		IntStatistics h = new IntStatistics(3);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		h.addValue(1);
		h.addValue(10);
		
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, -1) > 0.999);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 2) > 0.6);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 3) > 0.45);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 3) < h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 2));
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 12) < 0.001);
	}
	
	/**
	 * Make sure that LESS_THAN_OR_EQ binning does something reasonable.
	 */
	@Test public void opLessThanOrEqualsTest() {
		IntStatistics h = new IntStatistics(10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		h.addValue(1);
		h.addValue(10);
		
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, -1) < 0.001);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 2) < h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 3));
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 3) < h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 4));
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 12) > 0.999);
	}
	
	/**
	 * Make sure that equality binning does something reasonable.
	 */
	@Test public void opNotEqualsTest() {
		IntStatistics h = new IntStatistics(100);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		
		// Be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.NOT_EQUALS, 3) < 0.001);
		Assert.assertTrue(h.estimateSelectivity(Op.NOT_EQUALS, 8) > 0.01);
	}
}