package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private final int bins[];
    private final int min;
    private final int max;
    private volatile int ntups;
    private volatile double avgSelectivity;

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
    	bins = new int[buckets];
        this.min = min;
        this.max = max;
        this.avgSelectivity=0.0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int nowNTups = ntups;//read volatile == synchronized enter, only for data visibility, no mutex guaranteed
        int buckSteps = (int)((max-min)/bins.length);
        if (buckSteps == 0)
            buckSteps++;

        int buck = (v - min)/buckSteps;
        if (buck >= bins.length) buck = bins.length-1;
        int old=bins[buck];
        bins[buck]++;
        
        this.avgSelectivity+=2*old+1;
        ntups=nowNTups+1;//write volatile == synchronized leave

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
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        return this.avgSelectivity/this.ntups/this.ntups;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        int nowNTups = ntups;//read volatile == synchronized enter
        int buckSteps = (int)((max-min)/bins.length);
        if (buckSteps == 0)
            buckSteps++;

        int start = min;
        String s = "";
        for (int i = 0; i < bins.length; i++) {
            s += "BIN " + i + " START " + start + " END " + (start + buckSteps) + " HEIGHT " + bins[i] + "\n";
            start += buckSteps;
        }
        return s;
    }
}
