import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CrimesReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int crimeCount = 0;
        for (Text val : values) {
            crimeCount += 1;
        }
        context.write(key, new Text("Count: " + crimeCount));
    }

}
