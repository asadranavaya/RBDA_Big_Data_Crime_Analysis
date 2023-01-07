package dataingestion;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueCrimesMapper extends Mapper<LongWritable, Text, Text, Text> {
    // Input Structure
    // mapOutput.append(line[0] + ","); // Arrest Key
    // mapOutput.append(line[1] + ","); // Arrest Date
    // mapOutput.append(line[2] + ","); // Arrest classification code
    // mapOutput.append(line[3] + ","); // Arrest classification description
    // mapOutput.append(line[8] + ","); // Arrest bourough
    // mapOutput.append(line[16] + ","); // Latitude
    // mapOutput.append(line[17]); // Longitude

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split(",");
        // Check if there is an error in the csv
        String outString = null;
        final String outKey = line[2];
        if (line[3].equals("")) {
            outString = "Not Provided";
        } else {
            outString = line[3];
        }
        context.getCounter("Dyanmic_Bourough_Count", line[4].toLowerCase()).increment(1);
        context.write(new Text(outKey), new Text(outString + "," + 1));
    }
}