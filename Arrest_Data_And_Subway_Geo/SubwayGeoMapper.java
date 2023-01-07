package dataingestion;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SubwayGeoMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Go through each line
        // Set line as array delimited by space char
        String[] line = value.toString().split(",");
        // Check if there is an error in the csv

        // build output
        StringBuilder mapOutput = new StringBuilder();
        mapOutput.append(line[1] + ","); // Subway number
        mapOutput.append(line[2] + ","); // Subway Name
        final String[] latLong = line[3].substring(7, line[3].length() - 1).split(" ");
        mapOutput.append(latLong[0] + ","); // Longitude
        mapOutput.append(latLong[1]); // Latitude

        context.write(NullWritable.get(), new Text(mapOutput.toString()));
    }
}