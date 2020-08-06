import org.apache.commons.math.stat.correlation.PearsonsCorrelation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DriverClass {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path out = new Path(args[1]);

        Job job1 = Job.getInstance(conf, "count number of schools per zip");

        job1.setJarByClass(DriverClass.class);
        job1.setMapperClass(MapperClass.class);
        job1.setReducerClass(ReducerClass.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(out, "out1"));
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Gross income for every zip code");

        job2.setJarByClass(DriverClass.class);
        job2.setMapperClass(SecondMapper.class);
        job2.setReducerClass(SecondReducer.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job3 = Job.getInstance(conf, "join and pearson coefficient");

        job3.setJarByClass(DriverClass.class);

        job3.setReducerClass(JoinAndCalculateCoefficientReducer.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(CustomWritable.class);

        MultipleInputs.addInputPath(job3, new Path(out, "out1"), SequenceFileInputFormat.class, ZipMapper.class);
        MultipleInputs.addInputPath(job3, new Path(out, "out2"), SequenceFileInputFormat.class, AgiMapper.class);

        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job3, new Path(out, "out3"));
        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}
