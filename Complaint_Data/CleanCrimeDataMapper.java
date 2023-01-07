import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanCrimeDataMapper
    extends Mapper<LongWritable, Text, Text, Text> {
 
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
        String row = value.toString();
        String[] columns = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    
        // remove rows with columns less than 35 (i.e. incorrect data)
        if (columns.length <= 34) {
          return;
        }
    
        String id = columns[0];
        String date = columns[1];
        String time = columns[2];
        String offense_code = columns[7];
        String offense_description = columns[8];
        String level_of_offense = columns[12];
        String premise_type = columns[15];
        String lat_long = columns[29];
        String station_name = columns[31];
    
        if(id.isEmpty() || date.isEmpty() || time.isEmpty() || offense_code.isEmpty() || level_of_offense.isEmpty() || lat_long.isEmpty()) {
          return;
        }

        if(level_of_offense.equals("FELONY")) {
          String output = id + "," + date + "," + time + "," + offense_code + "," + offense_description + "," + level_of_offense + "," + premise_type + "," + lat_long + "," + station_name;
          context.write(new Text(id), new Text(output));
        }


  }
}
