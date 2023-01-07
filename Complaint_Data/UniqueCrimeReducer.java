import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.TreeMap;
import java.util.Map;

public class UniqueCrimeReducer extends Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
        //Input: <offense_code, offense_description, 1>

      int count=0;  
        String offense_description = "";
      for (Text value: values) {
        if(offense_description.isEmpty()) {
            offense_description = value.toString().split(",")[0];
        }
        count++;
        
      }
      context.write(key, new Text(offense_description + ":" + count));
  }
}