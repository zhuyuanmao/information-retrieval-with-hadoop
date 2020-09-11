package part1;


import java.util.StringTokenizer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class PosTermRecorder extends Configured implements Tool {

	
	public static void main(String[] args) throws Exception {
		int rs = ToolRunner.run(new PosTermRecorder(), args);
		System.exit(rs);
	}
	//Mapreduce program
	//Target: To get positional index for term-docid in a corpus of plain text documents

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "PosTermRecorder");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
		String doc_content = lineText.toString();
		
		Text term_doc = new Text();
		Text pos = new Text();
		StringTokenizer st = new StringTokenizer(doc_content," ");
		
		String docName = context.getInputSplit().toString();
		docName = docName.substring(docName.lastIndexOf("/") + 1);
		String docID = docName.split("_")[1];
		while(st.hasMoreElements()){
			String temp = st.nextElement().toString();
			String word = NormalizeWord(temp);
			
			if (word == null)
				continue;
			
			for(int j = 0;j<words.length;j++){
							
				if(words[j].toLowerCase().equals(temp.toLowerCase())){
					Stemmer s = new Stemmer();
					s.add(word.toCharArray(), word.length());
					s.stem();											//Porter Stemmer. Normalizing term
					term_doc = new Text(s.toString()+ "\t"+docID);
					pos = new Text(Integer.toString(j+1));
					context.write(term_doc, pos);
						
					}
			}
		
					
			
			
			//term_doc = new Text(word+ ","+docID+"\t");
			
		}
		
	}
}
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text word, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		sb.append("< ");
		boolean first = true;
		for (Text value : values) {
			if (first) {
				sb.append(value.toString());
				first = false;
			}
			if (value == null)
				continue;
			if (sb.lastIndexOf(value.toString()) < 0) {
				sb.append(", ");
				sb.append(value.toString());
				}
			}
		sb.append(" >");
	
		context.write(word, new Text(sb.toString()));
		
	
	}
}
	//function: normalizedWord: covert given string to lower case and remove all special characters
	//Parameter: String word
	//Return: normalized word or null.
	public static String NormalizeWord(String word) {
    String normalizedWord = word.toLowerCase();
    normalizedWord = normalizedWord.replaceAll("[^a-z0-9]+","");
  
    if (normalizedWord != null && !normalizedWord.equals("")){
    	return normalizedWord;
    }
    else{
        return null;
    }

	}
	
	
}
	    
		


