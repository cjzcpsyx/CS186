package simpledb;

/** A class to represent statistics for a single integer-based field.
 */
public class IntStatistics {

    // You made add any other fields you think are necessary.

    private int numTuples;
    private int numDistinctTuples;
    private final boolean[] distinctInts;

    // TODO: IMPLEMENT ME
    private Integer high;
    private Integer low;

    /**
     * Create a new IntStatistic.
     * 
     * This IntStatistic should maintain a statistics about the integer values that it receives.
     * 
     * The integer values will be provided one-at-a-time through the "addValue()" function.
     */
    public IntStatistics(int bins) {
        numTuples = 0;
        numDistinctTuples = 0;
        distinctInts = new boolean[bins];

        // TODO: IMPLEMENT ME
        high = null;
        low = null;
    }

    /**
     * Add a value to the set of values that you are tracking statistics for
     * @param v Value to add to the statistics
     */
    public void addValue(int v) {
        // TODO: IMPLEMENT ME
        if (high == null || high < v) {
            high = v;
        }

        if (low == null || low > v) {
            low = v;
        }

        // hashes the value and keeps an estimate to the number of distinct tuples we've seen
        int index = (hashCode(v) % distinctInts.length + distinctInts.length) % distinctInts.length;
        if (distinctInts[index] == false) {
            distinctInts[index] = true;
            numDistinctTuples++;
        }

        numTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // the approximate number of distinct tuples we've seen in total
        double numDistinct = ((double) numTuples) * numDistinctTuples / distinctInts.length;

        // TODO: IMPLEMENT ME
        switch (op) {
        case EQUALS:
            if (v > high || v < low) {
                return 0;
            }
            return (double)1 / numDistinct;
        case NOT_EQUALS:
            return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
        case GREATER_THAN:
            if (v < low) {
                return 1;
            }
            if (v > high) {
                return 0;
            }
            return (double)(high - v) / (high - low);
        case GREATER_THAN_OR_EQ:
            return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
        case LESS_THAN:
            if (v < low) {
                return 0;
            }
            if (v > high) {
                return 1;
            }
            return (double)(v - low) / (high - low);
        case LESS_THAN_OR_EQ:
            return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
        case LIKE:
            return estimateSelectivity(Predicate.Op.EQUALS, v);
        }
        return -1.0;
    }

    /**
     * Helper function to make a good hash value of an integer
     */
    static int hashCode(int v) {
        v ^= (v >>> 20) ^ (v >>> 12);
        return v ^ (v >>> 7) ^ (v >>> 4);
    }
}
