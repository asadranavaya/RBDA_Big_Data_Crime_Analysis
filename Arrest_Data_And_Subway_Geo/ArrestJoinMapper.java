package dataingestion;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class ArrestJoinMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    public static final double LAT_LONG_RANGE = 0.0005;

    public Map<String, double[]> subwayLocations = new HashMap<String, double[]>();
    public static final Log LOG = LogFactory.getLog(SubwayJoinMapper.class);

    @Override
    public void setup(Context context) throws IOException {
        // Learned how to implement cache files from
        // https://www.geeksforgeeks.org/distributed-cache-in-hadoop-mapreduce/
        // https://github.com/nicomak/blog/blob/master/donors/src/main/java/mapreduce/join/replicated/ReplicatedJoinBasic.java
        // https://buhrmann.github.io/hadoop-distributed-cache.html

        URI[] files = context.getCacheFiles();
        if (files != null) {
            LOG.info("URI not empty");
            for (URI uri : files) {
                LOG.info(uri);
            }
            try (BufferedReader reader = new BufferedReader(new FileReader("label1"))) {
                String line = "";
                while ((line = reader.readLine()) != null) {
                    final String[] temp = line.split(",");
                    final double[] addToMap = new double[2];
                    addToMap[0] = Double.parseDouble(temp[3]);
                    addToMap[1] = Double.parseDouble(temp[2]);
                    subwayLocations.put(temp[0], addToMap);
                }
            }
        } else {
            LOG.error("uri is empty");
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Read in crimes, check if long and lat is within range, out put key and code
        if (subwayLocations.isEmpty()) {
            LOG.error("CACHED FILE FAILED TO READ, FAILING HARD");
            return;
        }
        final String[] temp = value.toString().split(",");
        final double latitude = Double.parseDouble(temp[5]);
        final double longitude = Double.parseDouble(temp[6]);
        loop: for (Map.Entry<String, double[]> entry : subwayLocations.entrySet()) {
            // Check Latitude
            if (latitude <= entry.getValue()[0] + LAT_LONG_RANGE && latitude >= entry.getValue()[0] - LAT_LONG_RANGE) {
                // Check Longitude
                if (longitude <= entry.getValue()[1] + LAT_LONG_RANGE
                        && longitude >= entry.getValue()[1] - LAT_LONG_RANGE) {
                    context.write(NullWritable.get(), new Text(value.toString() + "," + entry.getKey()));
                    break loop;
                }
            }
        }
    }
}
