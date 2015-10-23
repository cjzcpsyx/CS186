package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int min;
    private final int max;
    private double width;
    private int numTuples;
    private int[] counts;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.width = ((double) max - min) / buckets;
        if (width < 1) {
            width = 1;
            buckets = max - min;
        }
        numTuples = 0;
        counts = new int[buckets];
        this.min = min;
        this.max = max;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        counts[bucketIndex(v)]++;
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
        if (Predicate.Op.LESS_THAN_OR_EQ.equals(op)) v++;
        //First we normalize v to be in the range [min, max]
        if(v < min || v > max){
            if(op == Predicate.Op.EQUALS || op == Predicate.Op.LIKE)
                return 0;
            else if (op == Predicate.Op.NOT_EQUALS)
                return 1;
            else
                v = (v > max) ? max : min;
        }
        int index = bucketIndex(v);

        if (Predicate.Op.EQUALS.equals(op)) {
            return ((double) counts[index]) / width / numTuples;
        } else if (Predicate.Op.GREATER_THAN.equals(op) || Predicate.Op.GREATER_THAN_OR_EQ.equals(op)) {
            double selectivity = (right(index) - v) * counts[index];
            for (int i = index + 1; i < counts.length; i++) {
                selectivity += counts[i];
            }
            return selectivity / width / numTuples;
        } else if (Predicate.Op.LESS_THAN.equals(op) || Predicate.Op.LESS_THAN_OR_EQ.equals(op)) {
            double selectivity = (v - left(index)) * counts[index];
            for (int i = 0; i < index; i++) {
                selectivity += counts[i];
            }
            return selectivity / width / numTuples;
        } else if (Predicate.Op.NOT_EQUALS.equals(op)) {
            return 1 - ((double) counts[index]) / width / numTuples;
        }
        // Huh?
        return -1;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        // optional: implement for a more nuanced estimation, or for skillz
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return Arrays.toString(counts);
    }

    private int bucketIndex(int v) {
        int index = (int) ((v - min) / width);
        if (index < 0) return 0;
        if (index >= counts.length) return counts.length - 1;
        else return index;
    }

    private double left(int index) {
        return min + width * index;
    }

    private double right(int index) {
        return left(index + 1);
    }
}
