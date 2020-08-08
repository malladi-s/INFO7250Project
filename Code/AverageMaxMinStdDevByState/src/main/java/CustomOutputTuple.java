import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomOutputTuple implements Writable {
    private Double min;
    private Double max;
    private long count;
    private Double median;
    private Double stdDev;

    public CustomOutputTuple(){

    }

    public CustomOutputTuple(Double min, Double max, long count, Double median, Double stdDev) {
        this.min = min;
        this.max = max;
        this.count = count;
        this.median = median;
        this.stdDev = stdDev;
    }

    public Double getMin() {
        return min;
    }

    public void setMin(Double min) {
        this.min = min;
    }

    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public Double getMedian() {
        return median;
    }

    public void setMedian(Double median) {
        this.median = median;
    }

    public Double getStdDev() {
        return stdDev;
    }

    public void setStdDev(Double stdDev) {
        this.stdDev = stdDev;
    }

    @Override
    public String toString() {
        return "Sat min - " + min + " " + "Sat max - " + max + " " + "Sat median - " + median + " " + "Sat std dev - " + stdDev;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeLong(count);
        out.writeDouble((median));
        out.writeDouble(stdDev);
    }

    public void readFields(DataInput in) throws IOException {
        min = in.readDouble();
        max = in.readDouble();
        count = in.readLong();
        median = in.readDouble();
        stdDev = in.readDouble();
    }
}
