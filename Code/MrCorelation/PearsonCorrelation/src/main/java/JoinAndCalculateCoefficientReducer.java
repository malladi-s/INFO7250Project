import org.apache.commons.math.stat.correlation.PearsonsCorrelation;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinAndCalculateCoefficientReducer extends Reducer<IntWritable, Text, IntWritable, CustomWritable> {

    public static double[] convertDouble(List<Double> numbers) {
        double[] ret = new double[numbers.size()];
        for (int i=0; i < ret.length; i++) {
            ret[i] = numbers.get(i).intValue();
        }
        return ret;
    }


    private ArrayList<Double> countArray = new ArrayList<Double>();
    private ArrayList<Double> agiArray = new ArrayList<Double>();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        CustomWritable result = new CustomWritable();
        for(Text value: values) {
            String source = value.toString().split(" - ")[0];
            Integer keyValue = Integer.parseInt(value.toString().split(" - ")[1]);
            if(source.equals("agi")){
                result.setAgi(keyValue);
            } else {
                result.setCount(keyValue);
            }
        }

        if(result.getAgi() != null) {
            int x = result.getCount() == null ? 0 : result.getCount();
            int y = result.getAgi();
            int n = result.getNumber() + 1;

            countArray.add(new Double(x));
            double[] countDoubleArray = convertDouble(countArray);

            agiArray.add(new Double(y));
            double[] agiDoubleArray = convertDouble(agiArray);

            Double coefficient = Double.valueOf(1);
            if(countArray.size() > 1 && agiArray.size() > 1) {
                PearsonsCorrelation corr = new PearsonsCorrelation();
                coefficient = corr.correlation(countDoubleArray, agiDoubleArray);
            }

            result.setCoefficient(coefficient);
            result.setNumber(n);
            context.write(key, result);
        }
    }
}