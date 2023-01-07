package dataingestion;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IngestMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Go through each line
        // Set line as array delimited by space char
        String[] line = value.toString().split(",");
        // Check if there is an error in the csv
        if (line.length < 16) {
            return;
        }
        // Filter out non-felonies
        if (!line[7].equalsIgnoreCase("f")) {
            return;
        }
        // build output
        StringBuilder mapOutput = new StringBuilder();
        mapOutput.append(line[0] + ","); // Arrest Key
        mapOutput.append(line[1] + ","); // Arrest Date
        mapOutput.append(line[2] + ","); // Arrest classification code
        mapOutput.append(line[3] + ","); // Arrest classification code description
        mapOutput.append(line[8] + ","); // Arrest bourough
        mapOutput.append(line[16] + ","); // Latitude
        mapOutput.append(line[17]); // Longitude

        context.write(NullWritable.get(), new Text(mapOutput.toString()));
    }

}