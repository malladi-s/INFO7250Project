import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Integer output = 0;

        String line = value.toString();
        String[] tokens = line.split(",");

        String state = tokens[5];

        String stringEnrollmentCount = tokens[578];

        if(!stringEnrollmentCount.equals("NULL") && !stringEnrollmentCount.equals("UG12MN")) {
            Integer enrollment = Integer.parseInt(stringEnrollmentCount);

            output = enrollment;

            if(!state.equals("STABBR")) {
                context.write(new Text(state), new IntWritable(output));
            }
        }
    }
}
