import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimesMapper extends Mapper<LongWritable, Text, Text, Text> {



    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        // Iterate through each line
        // Set line as array delimited
        String[] line = value.toString().split(",");
        // Check if there is an error in the csv

        context.write(new Text(line[2]), new Text("1"));

    }

}