package uni.fmi.Cereal;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.net.URI;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public class App extends JFrame
{
	
	private void init() {
		
        JPanel panel = new JPanel();
		
		
		JLabel resultLabel = new JLabel("Резултат:");
		String[] resultOptions = {"калории за грам", "списък закуски"};
		final JComboBox<String> resultDropdown = new JComboBox<>(resultOptions);

		
		JLabel nameLabel = new JLabel("Търсене на закуска по име:");
	    final JTextField nameInput = new JTextField();

	   
		JLabel proteinLabel = new JLabel("По съдържание на протеин:");
		final JTextField proteinInput = new JTextField();

		
	    JLabel sugarLabel = new JLabel("По съдържание на захар:");
		final JTextField sugarInput = new JTextField();

		 
		JLabel caloriesLabel = new JLabel("Максимални калории:");
		final JTextField caloriesInput = new JTextField();

		JButton search = new JButton("Търси");
		
		panel.setLayout(null);
		
		panel.add(resultLabel);
	    panel.add(resultDropdown);
	    panel.add(nameLabel);
	    panel.add(nameInput);
	    panel.add(proteinLabel);
	    panel.add(proteinInput);
	    panel.add(sugarLabel);
	    panel.add(sugarInput);
	    panel.add(caloriesLabel);
	    panel.add(caloriesInput);
	    panel.add(search);
		
		
	    resultLabel.setBounds(70, 20, 100, 30);
	    resultDropdown.setBounds(180, 20, 200, 30);
	    nameLabel.setBounds(70, 70, 250, 30);
	    nameInput.setBounds(70, 100, 310, 30);
	    proteinLabel.setBounds(70, 140, 250, 30);
	    proteinInput.setBounds(70, 170, 310, 30);
	    sugarLabel.setBounds(70, 210, 250, 30);
	    sugarInput.setBounds(70, 240, 310, 30);
	    caloriesLabel.setBounds(70, 280, 250, 30);
	    caloriesInput.setBounds(70, 310, 310, 30);
	    
	    search.setBounds(145, 360, 150, 30);
	    
		add(panel);
		
		setBounds(300, 300, 440, 500);
		setVisible(true);
		
		resultDropdown.addActionListener(new ActionListener() {
		    @Override
		    public void actionPerformed(ActionEvent e) {
		        String selectedResult = (String) resultDropdown.getSelectedItem();
		        if (selectedResult.equals("калории за грам")) {
		            caloriesInput.setEnabled(false); // Деактивиране на полето
		        } else {
		            caloriesInput.setEnabled(true); // Активиране на полето
		        }
		    }
		});
		
        search.addActionListener(new ActionListener() {
			
			@Override
			public void actionPerformed(ActionEvent e) {
				
				String selectedResult = (String) resultDropdown.getSelectedItem();
				boolean isCaloriesPerGram = "калории за грам".equals(selectedResult);

				String text = nameInput.getText();
				
				double minProtein = 0.0;
		        double maxSugar = Double.MAX_VALUE;
		        double maxCalories = Double.MAX_VALUE;
				
		        if (!proteinInput.getText().isEmpty()) {
		            try {
		                minProtein = Double.parseDouble(proteinInput.getText());
		            } catch (NumberFormatException ex) {
		                JOptionPane.showMessageDialog(null, "Невалидни числови стойности.", "Грешка", JOptionPane.ERROR_MESSAGE);
		                return;
		            }
		        }
		        
		        if (!sugarInput.getText().isEmpty()) {
		            try {
		                maxSugar = Double.parseDouble(sugarInput.getText());
		            } catch (NumberFormatException ex) {
		                JOptionPane.showMessageDialog(null, "Невалидни числови стойности.", "Грешка", JOptionPane.ERROR_MESSAGE);
		                return;
		            }
		        }

		        if (!caloriesInput.getText().isEmpty()) {
		            try {
		                maxCalories = Double.parseDouble(caloriesInput.getText());
		            } catch (NumberFormatException ex) {
		                JOptionPane.showMessageDialog(null, "Невалидни числови стойности.", "Грешка", JOptionPane.ERROR_MESSAGE);
		                return;
		            }
		        }
		        
		        
		        if (text.isEmpty() && minProtein <= 0.0 && maxSugar >= Double.MAX_VALUE && maxCalories >= Double.MAX_VALUE) {
		            JOptionPane.showMessageDialog(null, "Въведете поне един критерий за търсене.", "Грешка", JOptionPane.ERROR_MESSAGE);
		            return;
		        }
			
		        startHadoop(text, isCaloriesPerGram, minProtein, maxSugar, maxCalories);
			}
		});
		
	}
	

	protected void startHadoop(String text,  boolean isCaloriesPerGram, double minProtein, double maxSugar, double maxCalories) {
		
        Configuration conf = new Configuration();
    	
    	JobConf job = new JobConf(conf, App.class);
    	job.set("name", text);
    	job.setBoolean("isCaloriesPerGram", isCaloriesPerGram);
    	job.setDouble("minProtein", minProtein); 
        job.setDouble("maxSugar", maxSugar); 
    	job.setDouble("maxCalories", maxCalories); 

    	
    	if (isCaloriesPerGram) {
            job.setMapperClass(CaloriesPerGramMapper.class);
            job.setReducerClass(CaloriesPerGramReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
        } else {
            job.setMapperClass(SnackListMapper.class);
            job.setReducerClass(SnackListReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
        }
		
    	Path inputPath = new Path("hdfs://127.0.0.1:9000/cereal.csv");
   	    Path outputPath = new Path("hdfs://127.0.0.1:9000/cereal");
   	  
   	    FileInputFormat.setInputPaths(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);

	    
	    try {
	    	   FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);

	    	   if (fs.exists(outputPath)) {
	    	    fs.delete(outputPath, true);
	    	   }

	    	   RunningJob task = JobClient.runJob(job);
	    	   System.out.println("Success=" + task.isSuccessful());
	    	  } catch (IOException e) {
	    	   
	    	   e.printStackTrace();
	    	  }
	}


	public static void main( String[] args )
    {
    	App from = new App();
        from.init();
    }

	
}
