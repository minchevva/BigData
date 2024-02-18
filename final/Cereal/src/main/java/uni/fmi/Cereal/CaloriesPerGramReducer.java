package uni.fmi.Cereal;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CaloriesPerGramReducer extends MapReduceBase 
implements Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	
	boolean isCaloriesPerGram;  
	double minProtein;
    double maxSugar;
    double maxCalories;
    
    @Override
    public void configure(JobConf job) {
    	 isCaloriesPerGram = job.getBoolean("isCaloriesPerGram", false);
    	 minProtein = job.getFloat("minProtein", 0.0f);
         maxSugar = job.getFloat("maxSugar", Float.MAX_VALUE);
         maxCalories = job.getFloat("maxCalories", Float.MAX_VALUE);
    }
    
    
	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {


		String currentNameCereal = key.toString();
		
        double weight = 0.0;
        double calories = 0.0;
        
        while (values.hasNext()) {
            double value = values.next().get();
            if (value > 0.0) {
                if (weight == 0.0) {
                    weight = value;
                } else if (calories == 0.0) {
                    calories = value;
                }
            }
        }
        
        if ( weight > 0.0 && calories > 0.0) {
            double caloriesPerGram = weight / calories;
            
            Text outputKey = new Text(currentNameCereal + " -  ");
            DoubleWritable outputValue = new DoubleWritable(caloriesPerGram);

          
            output.collect(outputKey, outputValue);
            
               }
		
	}

}
