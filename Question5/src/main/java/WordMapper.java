
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/*
	 * The map method runs once for each line of text in the input file.
	 * The method receives a key of type LongWritable, a value of type
	 * Text, and a Context object.
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {

	//deletes inline commas so that the string values can be parsed through csv
		
	String rawline = value.toString();
	String line = rawline.replace(", ", " ");
	
	String[] data = line.split(",");
	
	if (data[2].contains("Government expenditure on education")) {
		int counter = 0; 
		double total = 0;
		for (int i = 4; i < data.length; i++) {
			if (!data[i].equals("\"\"")) {
				try {
					String temp = data[i].substring(1, data[i].length() - 1);
					total += Double.parseDouble(temp);
					counter++;
				}
				catch (Exception e) {
					
				}
			}
		}
		if (counter != 0) {
			double average = total/counter;
			context.write(new Text(data[0]), new IntWritable((int) average));
		}
		
	}
	
		
	}
}