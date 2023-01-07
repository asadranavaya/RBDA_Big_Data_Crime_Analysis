package dataingestion;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SubwayJoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        final Map<Integer, Integer> crimeCount = new TreeMap<Integer, Integer>();
        int numEmpty = 0;
        for (Text val : values) {
            final String[] line = val.toString().split(",");
            if (line[0].equals("")) {
                numEmpty++;
            } else {
                int keyCode = Integer.parseInt(line[0]);
                if (!crimeCount.containsKey(keyCode)) {
                    crimeCount.put(keyCode, 1);
                } else {
                    crimeCount.replace(keyCode, crimeCount.get(keyCode) + 1);
                }
            }
        }
        if (numEmpty > 0)
            crimeCount.put(-1, numEmpty);
        // Write subway with keycode of crime followed by count, strip off {} added by
        // treemap with a substring
        context.write(key, new Text(crimeCount.toString().substring(1, crimeCount.toString().length() - 1)));
    }

}
