import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MapperClass extends Mapper<LongWritable, Text, Text, TupleWritableClass> {
    TupleWritableClass tuple = new TupleWritableClass();
    Text state = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split(",");

        state.set(tokens[5]);

        Double satScore = null;

        try {
            satScore = Double.parseDouble(tokens[22]);
            if(state != null && satScore != null) {
                tuple.setMin(satScore);
                tuple.setMax(satScore);
                tuple.setCount(1);
            }
            context.write(state, tuple);
        } catch (Exception e) {
            System.out.println(e);
        }
        

    }
}
