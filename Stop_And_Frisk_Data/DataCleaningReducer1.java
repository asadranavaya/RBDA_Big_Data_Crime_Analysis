import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import java.util.TreeMap;
import java.util.Map;

public class DataCleaningReducer1 extends Reducer<NullWritable, Text, NullWritable, Text> {
  public void reduce(Text key, Iterable<Text> infos, Context context) throws IOException, InterruptedException {  
      for (Text info: infos) {
        context.write(NullWritable.get(), info);
      }
  }
}