import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AgiMapper extends Mapper<IntWritable, IntWritable, IntWritable, Text> {
    @Override
    public void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        Text result = new Text("agi - " + value);
        context.write(key, result);
    }
}