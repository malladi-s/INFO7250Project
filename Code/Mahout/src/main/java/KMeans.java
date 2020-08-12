import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

public class KMeans {
    private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "data/Kmeansdata";
    private static final String DIRECTORY_INPUT = "KmeansTest";
    private static final String DIRECTORY_OUTPUT = "KmeansOutput";

    public static void main(String[] args) throws Exception {
        Path output = new Path(DIRECTORY_OUTPUT);

        // Hadoop configuration details
        Configuration conf = new Configuration();
        HadoopUtil.delete(conf, output);

        run(conf, new Path(DIRECTORY_INPUT), output, new EuclideanDistanceMeasure(), 3, 0.5, 10);
    }

    public static void run(Configuration conf, Path input, Path output, DistanceMeasure measure, int k,
                           double convergenceDelta, int maxIterations) throws Exception {

        Path directoryContainingConvertedInput = new Path(output, DIRECTORY_CONTAINING_CONVERTED_INPUT);
        InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");

        // Get initial clusters randomly
        Path clusters = new Path(output, "random-seeds");
        clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);

        // Run K-means with a given K
        KMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, convergenceDelta,
                maxIterations, true, 0.0, false);

        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs,new Path(output, Cluster.CLUSTERED_POINTS_DIR + "/part-m-00000"),
                conf);
        IntWritable key = new IntWritable();
        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
        while (reader.next(key, value)) {
            System.out.println("Cluster Center: " + key.toString()+ " Cluster member properties: "+ value.toString());
        }
        reader.close();
    }
}