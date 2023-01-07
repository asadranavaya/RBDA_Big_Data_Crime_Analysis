import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ViolentCrimeFilterMapper
    extends Mapper<LongWritable, Text, Text, Text> {
 
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
        String ViolentCrimes = "101,102,103,104,105,106,114,115,116,117,118,124,125,233,235,236,340,341,343,344,347,351,352,355,359,361,364,365,366,578";

        String row = value.toString();
        String[] columns = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    
        
        String offense_code = columns[3];
        if (ViolentCrimes.contains(offense_code) || offense_code.isEmpty()) {
            context.write(new Text(columns[0]), value);
        }
        
        


  }
}
