
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/*
	 * The map method runs once for each line of text in the input file.
	 * The method receives a key of type LongWritable, a value of type
	 * Text, and a Context object.
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {

		
	String rawline = value.toString();
	String line = rawline.replace(", ", " ");
	
	String[] data = line.split(",");

	//finds the row of data that specifies target data: The percent of graduating women, cumulative, in the country
	if (data[2].equals("\"Educational attainment at least Bachelor's or equivalent population 25+ female (%) (cumulative)\"")) {
		
		//find maximum. 
		
		double max = 0;
		for (int i = 4; i < data.length; i++) {
			
			
			if (!data[i].equals("\"\"")) {
				String temp = data[i].substring(1, data[i].length()-1);
				try {
					if (max < Double.parseDouble(temp)) {
						max = Double.parseDouble(temp);
					}
				}
				catch (Exception e) {
					
				}
			}
		}
		
		//checks if maximum graduation percentage over ALL years is less than 30%. 
		//if so, writes country key and graduation percent value to context
		if (max < 30) {
			context.write(new Text(data[0]), new IntWritable((int) max));
		}
			
	}
	else { //If not the selected string for column 3, return. aka skip it. 
		return; 
	}

		
}
}