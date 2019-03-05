
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;



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
	
	
	if (data[0].equals("\"United States\"")) {
		Double startyear = 1.000;
		Double endyear = 1.000;
		
		try {
			if (data[2].contains("Educational attainment") && data[2].contains("female")) {
				//we must be careful not to divide by zero. it could happen when
				//calculating a percentage
				if(!data[49].equals("\"\"") && !data[49].equals("\"0\"")) {
					//if there is non-zero data in earliest record, 2005
					
					String temp = data[49].substring(1, data[49].length() - 1);
					startyear = Double.parseDouble(temp);
					
					for (int i = 49; i < data.length; i++) {
						if(!data[i].equals("\"\"")) {
							temp = data[i].substring(1, data[i].length() - 1);
							endyear = Double.parseDouble(temp);
						}
					}
					
				}
				else if(!data[58].equals("\"\"") && !data[58].equals("\"0\"")) {
					//if there is non-zero data in earliest record, 2014
					
					String temp = data[58].substring(1, data[58].length() - 1);
					startyear = Double.parseDouble(temp);
					
					
					for (int i = 58; i < data.length; i++) {
						if(!data[i].equals("\"\"")) {
							temp = data[i].substring(1, data[i].length() - 1);
							endyear = Double.parseDouble(temp);
						}
					}
				}
				
				double percent = ((endyear / startyear)-1)*100; 
				
				String outputkey = data[2].substring(24, data[2].length()-2);
				
				context.write(new Text(outputkey), new IntWritable((int) percent)); 
			}
			
			
		}
		catch (Exception e) {
			
		}
		
			
	}
	else { //If not the selected string for column 3, return. aka skip it. 
		return; 
	}

		
}
}