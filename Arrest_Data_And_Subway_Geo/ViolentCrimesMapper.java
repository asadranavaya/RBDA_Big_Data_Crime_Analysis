package dataingestion;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ViolentCrimesMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    public static final String VIOLENT_CRIMES_KEYS = "100,104,105,106,107,112,117,119,123,124,126,153,155,157,164,166,168,176,177,178,179,180,184,185,186,187,196,197,198,199,261,268,440,513,522,579,582,583,586,639,640,645,647,648,663,664,665,672,691,693,694,695,696,697,749,764,776,777,781,783,790,792,793,795,801,844,916";
    // Input format
    // mapOutput.append(line[0] + ","); // Arrest Key
    // mapOutput.append(line[1] + ","); // Arrest Date
    // mapOutput.append(line[2] + ","); // Arrest classification code
    // mapOutput.append(line[3] + ","); // Arrest classification code description
    // mapOutput.append(line[8] + ","); // Arrest bourough
    // mapOutput.append(line[16] + ","); // Latitude
    // mapOutput.append(line[17]); // Longitude

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Go through each line
        // Set line as array delimited by space char
        String[] line = value.toString().split(",");
        // Filter out non-violent felonies
        if (VIOLENT_CRIMES_KEYS.contains(line[2]) || line[2].equals("")) {
            context.write(NullWritable.get(), value);
        }

    }

}