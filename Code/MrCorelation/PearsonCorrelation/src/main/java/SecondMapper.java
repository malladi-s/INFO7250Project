import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");

        String zipCode = tokens[2];
        String AGI = tokens[20];

        if(!zipCode.equals("ZIPCODE")) {
            Integer zip = Integer.parseInt(zipCode);
            if(zip != 0 && zip != 99999) {
                context.write(new IntWritable(zip), new IntWritable((int)Double.parseDouble(AGI)));
            }
        }
    }
}