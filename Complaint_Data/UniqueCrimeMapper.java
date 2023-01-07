import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueCrimeMapper
    extends Mapper<LongWritable, Text, Text, Text> {
 
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
        String row = value.toString();
        String[] columns = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    
    
        String offense_code = columns[3];
        String offense_description = columns[4];
    
        context.write(new Text(offense_code), new Text(offense_description+ ",1"));
        


  }
}
