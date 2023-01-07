import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HourlyCrimeCountMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static   IntWritable one = new IntWritable(1);
 
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
        
        String row = value.toString();
        String[] columns = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    
    
        String time = columns[2];
        String hour = time.split(":")[0];
    
        context.write(new Text(hour), one);
        


  }
}
