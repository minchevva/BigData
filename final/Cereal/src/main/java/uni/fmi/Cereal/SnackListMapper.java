package uni.fmi.Cereal;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SnackListMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, Text> {

	String name;
	double minProtein; 
    double maxSugar;     
    double maxCalories; 
    
    @Override
	public void configure(JobConf job) {
		
		name = job.get("name", "");
        minProtein = job.getDouble("minProtein", 0.0);
        maxSugar = job.getDouble("maxSugar", Double.MAX_VALUE);
        maxCalories = job.getDouble("maxCalories", Double.MAX_VALUE);
       	}
    
    
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {


String[] columns = value.toString().split(";");
		
        if (name.isEmpty() || columns[0].toLowerCase().contains(name.toLowerCase())) {
            try {
                String currentNameCereal = columns[0];
                double protein = Double.parseDouble(columns[4]);
                double sugar = Double.parseDouble(columns[9]);
                double calories = Double.parseDouble(columns[3]);

                if (protein >= minProtein && sugar <= maxSugar && calories <= maxCalories) {
                    String result = String.format("%.1f - %.1f - %.1f", protein, sugar, calories);
                    output.collect(new Text(currentNameCereal), new Text(result));
                }
            } catch (NumberFormatException ex) {
                System.err.println(value.toString());
            }
        }
	}

}
