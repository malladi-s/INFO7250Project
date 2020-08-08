import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ReducerClass extends Reducer<Text, TupleWritableClass, Text, CustomOutputTuple> {
    CustomOutputTuple result = new CustomOutputTuple();
    List<Double> salaries = new ArrayList<Double>();

    @Override
    protected void reduce(Text key, Iterable<TupleWritableClass> values, Context context) throws IOException, InterruptedException {
        result.setMin(null);
        result.setMax(null);
        result.setCount(0);
        result.setMedian(null);
        result.setStdDev(null);

        long count = 0;
        double sum = (double) 0;

        for(TupleWritableClass minMaxTuple : values){
            sum += minMaxTuple.getMax();
            salaries.add(minMaxTuple.getMax());

            if(result.getMin() == null || result.getMin()>minMaxTuple.getMin()){
                result.setMin(minMaxTuple.getMin());
            }
            if(result.getMax() == null || minMaxTuple.getMax() > result.getMax()){
                result.setMax(minMaxTuple.getMax());
            }

            count += minMaxTuple.getCount();
            result.setCount(count);
        }

        Collections.sort(salaries);
        int len = salaries.size();

        if(len%2 !=0){
            result.setMedian(salaries.get((int)len/2));
        } else{
            result.setMedian((salaries.get((len-1)/2) + salaries.get(len/2))/2.0);
        }

        double mean = sum/count;
        double stdDev = 0.0;
        for(Double salary : salaries){
            stdDev =  (salary-mean)*(salary-mean);
        }

        if(len == 1) {
            result.setStdDev((double) 0);
        } else {
            result.setStdDev(Math.sqrt(stdDev/(len-1)));
        }

        context.write(key, result);
    }
}
