package uni.fmi.Cereal;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CaloriesPerGramMapper extends MapReduceBase 
implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	
	String name;
	boolean isCaloriesPerGram;
	double minProtein; 
    double maxSugar;     
    double maxCalories; 
	
	@Override
	public void configure(JobConf job) {
		
		name = job.get("name", "");
		isCaloriesPerGram = job.getBoolean("isCaloriesPerGram", false);
		
        minProtein = job.getDouble("minProtein", 0.0);
        maxSugar = job.getDouble("maxSugar", Double.MAX_VALUE);
        maxCalories = job.getDouble("maxCalories", Double.MAX_VALUE);
        
		}
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {


		String[]columns = value.toString().split(";");
		
		if (name.isEmpty() ||
				columns[0].toLowerCase().contains(name.toLowerCase())
				) {
			
			try {
				String currentNameCereal = columns[0];
				double calories = Double.parseDouble(columns[3]);
				double weight = Double.parseDouble(columns[13]);
				
				if (isCaloriesPerGram && weight > 0.0 && calories > 0.0) {
                  
					output.collect(new Text(currentNameCereal), new DoubleWritable(weight));
                    output.collect(new Text(currentNameCereal), new DoubleWritable(calories));
                }
				
				    
			}catch(NumberFormatException ex){
				System.err.println(value.toString());
			}
		}
		
	}

}
