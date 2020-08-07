import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Comparator;

public class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
    public Map<String, Integer > map = new LinkedHashMap<String , Integer>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Integer result = 0;

        for(IntWritable value: values) {
            result += value.get();
        }

        map.put(key.toString(), result);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(map.entrySet());
        Collections.sort(list, new cm());//IMP

        HashMap<String, Integer> sorted = new LinkedHashMap<String, Integer>();
        for(Map.Entry<String, Integer> en: list){
            sorted.put(en.getKey(),en.getValue());
        }

        for (Map.Entry<String,Integer> entry : sorted.entrySet()){
            context.write(new Text(entry.getKey()),new IntWritable(entry.getValue()));
        }
    }

    class cm implements Comparator<Map.Entry<String, Integer>>{
        public int compare(Map.Entry<String, Integer> a,
                           Map.Entry<String, Integer> b) {
            return (b.getValue()).compareTo(a.getValue());
        }
    }
}
