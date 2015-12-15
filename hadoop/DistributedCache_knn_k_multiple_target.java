package stable_ballknn;

import java.io.*;
import java.net.URI;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import ballknn.k.DistributedCache_ballknn_multiple_target.DefinedComparator;
import ballknn.k.DistributedCache_ballknn_multiple_target.GroupingComparator;
import ballknn.k.DistributedCache_ballknn_multiple_target.KeyPartitioner;
import ballknn.k.DistributedCache_ballknn_multiple_target.TextPair;

public class DistributedCache_knn_k_multiple_target {
	// private BufferedReader modelBR = new BufferedReader(new
	// FileReader("/home/mjiang/java/eclipse/hadoop/Target-1/data/models.txt"));
	public static class Result{
		
		private double distance=0.0;
		private String vector=null;
		Result(double distance , String vector)
		{
			this.distance=distance;
			this.vector=vector;
		}
		
		
	}
	public static class TargetMapper extends Mapper<LongWritable, Text, TextPair, Text> {

		private Path[] modelPath;
		private BufferedReader modelBR;
		private ArrayList<String> R = new ArrayList<String>();// must be initialized with memory,or nullpointer 
		private String unlabeled = new String();
		
		private ArrayList<String> all_unlabeled=new ArrayList<String>();
		private ArrayList<ArrayList<Result>> topk_list=new ArrayList<ArrayList<Result>>();
		private int length_of_vector = 0;
		private int length_of_all_unlabeled=0;
		private int  number=0;
		//private ArrayList<Result> topk=new ArrayList<Result>();
		private double radius=0.0;
		private int samplenumber=0;
		
		private final TextPair tp=new TextPair();
		
		public void setup(Context context) throws IOException,InterruptedException {
			// Configuration conf = new Configuration(); /testS 为空
			Configuration conf = context.getConfiguration(); // testS 为空
			modelPath = DistributedCache.getLocalCacheFiles(conf);
			String line;
			//String[] tokens;
			BufferedReader joinReader = new BufferedReader(new FileReader(
					modelPath[0].toString()));
			try {
				while ((line = joinReader.readLine()) != null) {
					//unlabeled = line.toString();// if unlabeled is just one
					all_unlabeled.add(line.toString());
					
				}
				
				length_of_vector = all_unlabeled.get(0).split(",").length;
				length_of_all_unlabeled=all_unlabeled.size();
				//System.out.println("多少个位置类别"+length_of_all_unlabeled);
				/*String[] tt = all_unlabeled.get(0).split(",");
				lengthofvector = tt.length;*/
			} finally {
				joinReader.close();
			}
		}

		protected void  cleanup(Context context) throws IOException, InterruptedException
		{
			//context.w
			//int length_of_topk_list=all_unlabeled.size();
			
			//System.out.println("所有未分类的样本数"+topk_list.size()+"   ---------------第一个元素的个数"+topk_list.get(0).size());
			int lengthoftopk=topk_list.get(0).size();
			for(int j=0;j<length_of_all_unlabeled;j++){
				
				for(int i=0;i<lengthoftopk;i++)
				{
					try{
						tp.set(all_unlabeled.get(j),topk_list.get(j).get(i).distance);
						context.write(tp,new Text(topk_list.get(j).get(i).vector));
					}catch(Exception e)
					{
						e.getMessage();
					}
				}
			}
			
			super.cleanup(context);
		}
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException { // here we could do
															// computation
			// 获取model的文件
			
			
			String[] line = value.toString().split(",");
			
			for(int j=0;j<length_of_all_unlabeled;j++)
			//for(int j=0;j<1;j++)
			{
				// get the value of unlabeled
				double tmp_result = 0.0;
				String[] target = all_unlabeled.get(j).split(",");
				try {
					for (int i = 0; i < length_of_vector; i++) {
						tmp_result += Math.pow(Double.parseDouble(line[i]) - Double.parseDouble(target[i]), 2);

					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				// tp.set(unlabeled, tmp_result);

				// context.write(tp, value);

				if (samplenumber < 5) {
					
					/*if(topk_list.get(j)==null)
					{
						topk.add(new Result(tmp_result, value.toString()));
						topk_list.add(topk);
						topk.clear();
					}else{
						topk_list.get(j).add(new Result(tmp_result, value.toString()));
					}*/
					
					
					
					if(topk_list.size()<length_of_all_unlabeled)
					{
						
						
						ArrayList<Result> topk=new ArrayList<Result>();
						topk.add(new Result(tmp_result, value.toString()));
						topk_list.add(topk);
						//topk.clear();
						//topk=null;
					}else{
						topk_list.get(j).add(new Result(tmp_result, value.toString()));
					}
				} else {
					double max = 0.0;
					int counter = -1;
					for (int i = 0; i < 5; i++) {
						//System.out.println("samplenumber------------------------"+topk_list.get(j).size());
						if (topk_list.get(j).get(i).distance >= max) {
							max = topk_list.get(j).get(i).distance;
							counter = i;
						}

					}

					if (max > tmp_result) {
						topk_list.get(j).remove(counter);// how to make sure that just
												// remove and add one
						topk_list.get(j).add(new Result(tmp_result, value.toString()));
					}
				}
				
			}
			samplenumber++;
			/*if(samplenumber==4000){
				for(int i=0;i<5;i++)
				{
					context.write(new DoubleWritable(topk.get(i).distance),new Text(topk.get(i).vector));
				}
			}*/
		}
	}
	
	
	public static class TextPair implements WritableComparable<TextPair> {
		  private String first = null;
		  private double second = 0.0;

		  public void set(String left, double right) {
		    first = left;
		    second = right;
		  }
		  public String getFirst() {
		    return first;
		  }
		  public double getSecond() {
		    return second;
		  }

		  @Override
		  public void readFields(DataInput in) throws IOException {
			  first=in.readUTF();
		    second = in.readDouble();
		  }
		  @Override
		  public void write(DataOutput out) throws IOException {
		    out.writeUTF(first);;
		    out.writeDouble(second);
		  }
		  @Override
		  public int hashCode() {
		    return String.valueOf(first).hashCode() + String.valueOf(second).hashCode();
		  }
		  @Override
		  public boolean equals(Object right) {
		    if (right instanceof TextPair) {
		      TextPair r = (TextPair) right;
		      return r.first == first && r.second == second;
		    } else {
		      return false;
		    }
		  }
		  //这里的代码是关键，因为对key排序时，调用的就是这个compareTo方法
		  @Override
		  public int compareTo(TextPair o) {
			  if (!first.equalsIgnoreCase(o.first)) {
			      return first.compareTo(o.first);
			    } else if (second != o.second) {
			      //return new Double(second - o.second).intValue();
			      return (int)Math.signum(second - o.second);
			    } else {
			      return 0;
			    }
			  
			  /*if(second != o.second)
			  {
				  return (int)Math.signum(second - o.second);
			  }else
			  {
				  return 0;
			  }*/
		    /*if (first != o.first) {
		      return Math.signum(first - o.first);
		    } else if (second != o.second) {
		      return second - o.second;
		    } else {
		      return 0;
		    }*/
		  }
		}
	
	
	/*public static class GroupingComparator implements RawComparator<TextPair> {
		  @Override
		  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		    return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, b2, s2, Integer.SIZE/8);
		  }
		protected GroupingComparator()
        {
            super(TextPair.class, true);
        }
		  @Override
		  public int compare(TextPair o1, TextPair o2) {
		    String first1 = o1.getFirst();
		    String first2 = o2.getFirst();
		    //return first1.hashCode() - first2.hashCode();
		    return first1.compareTo(first2);
		  }
		}*/
	
	
	public static class GroupingComparator extends WritableComparator {
		 
		  protected GroupingComparator()
	        {
	            super(TextPair.class, true);
	        }

		  @Override
		  public int compare(WritableComparable o1_w, WritableComparable o2_w) {
			  TextPair o1=(TextPair)o1_w;
			  TextPair o2=(TextPair)o2_w;
		    String first1 = o1.getFirst();
		    String first2 = o2.getFirst();
		    return first1.compareTo(first2);
		  }
		}
	
	
	
	
	
	public class KeyPartitioner extends Partitioner<TextPair, Text> {

		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
	
	public static class DefinedComparator extends WritableComparator {//原来一个static 都可以热出那么多的事端啊
	    public DefinedComparator() {
	        super(TextPair.class,true);
	    }
	    
	    @Override
	    public int compare(WritableComparable combinationKeyOne, WritableComparable CombinationKeyOther) {
	                                                                                                                                                                                             
	        TextPair c1 = (TextPair) combinationKeyOne;
	        TextPair c2 = (TextPair) CombinationKeyOther;
	        if(!c1.getFirst().equals(c2.getFirst())){
	            return c1.getFirst().compareTo(c2.getFirst());
	            }
	        else{//按照组合键的第二个键的升序排序，将c1和c2倒过来则是按照数字的降序排序(假设2)
	            return (int)Math.signum(c1.getSecond()-c2.getSecond());
	        	//return Math.abs(c1.getSecond()-c2.getSecond());//0,负数,正数
	            //Math.abs
	        }
	        }
	}
	
	
	
	
	
	

	public static class combiner extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

		}
	}

	static class TargetReducer extends Reducer<TextPair, Text, DoubleWritable, Text> {
		public void reduce(TextPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//System.out.println("key.second===="+key.second);
			int k=0;
			for(Text value:values){
				context.write(new DoubleWritable(key.getSecond()), value);
				k++;
				if(k==5)
				{
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = new Job();
		Configuration conf = job.getConfiguration();
		//conf.setBoolean("wordcount.skip.patterns", true);
		
		if(args.length<1)
		{
			System.out.println("参数个数不匹配");
			return;
		}
		
		
		//DistributedCache.addCacheFile(new Path("/home/2.txt").toUri(), conf);
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);
		
		
		//DistributedCache.addCacheFile(new Path("/home/2.txt").toUri(), conf);

		job.setJarByClass(DistributedCache_knn_k_multiple_target.class);

		FileInputFormat.addInputPath(job, new Path("/home/twitter.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/out"));

		job.setInputFormatClass(TextInputFormat.class);
		
		
		job.setMapperClass(TargetMapper.class);
		/*TextInputFormat.setMinInputSplitSize(job,1024L);//设置最小分片大小
		TextInputFormat.setMaxInputSplitSize(job,2*1024*1024*10L);//设置最大分片大小 
*/
		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setSortComparatorClass(DefinedComparator.class);
		
		
		

		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		//job.setGroupingComparatorClass(GroupingComparator.class);
		//job.setNumReduceTasks(0);
		job.setReducerClass(TargetReducer.class);
		
		
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);

		//boolean test = conf.getBoolean("wordcount.skip.patterns", false);
		// MapContext jobConf= = new MapContext(conf, null);
		//Path[] modelPath = DistributedCache.getLocalCacheFiles(conf);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
