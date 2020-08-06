import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");

        String zipCode = tokens[6];

        if(!zipCode.equals("ZIP")) {
            if(zipCode.contains("-")) {
                zipCode = zipCode.split("-")[0];
            }
            context.write(new IntWritable(Integer.parseInt(zipCode)), new IntWritable(1));
        }
    }
}