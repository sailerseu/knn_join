package spark1.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import java.util.List;

import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.Accumulator;

public class Spark_ball_knn {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//SparkConf conf=new SparkConf().setAppName("spark knn").setMaster("spark://223.3.92.151:7077"); 
		SparkConf conf=new SparkConf().setAppName("spark ball knn").setMaster("yarn-cluster"); 
		JavaSparkContext sc=new JavaSparkContext(conf); 
		
		
		if(args.length<1)
		{
			System.out.println("please determined the target");
			return;
		}
		JavaRDD<String> lines = sc.textFile("hdfs://master:8020"+args[0]);
		//JavaRDD<String> lines = sc.textFile("hdfs://master:8020/home/3.txt");
		JavaRDD<String> t = sc.textFile("hdfs://master:8020/home/twitter.txt");
		
		JavaPairRDD<String,String> rt=lines.cartesian(t);
		//JavaPairRDD<String,String> rt=t.cartesian(lines);
		JavaPairRDD<String,Iterable<String>> rr=rt.mapToPair(new PairFunction<Tuple2<String,String>,String,String>(){
			@Override
			public Tuple2<String,String> call(Tuple2<String,String> t)
			{
				return new Tuple2<String,String>(t._1,t._1+"|"+t._2);
			}
			
		}).groupByKey();		
		
		//.flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() 
		JavaPairRDD<Double,String> sim=rr.values().flatMapToPair(new PairFlatMapFunction<Iterable<String>,Double,String>(){
		//JavaPairRDD<String,Double> sim=rr.values().flatMapToPair(new PairFlatMapFunction<Iterable<String>,String,Double>(){
			@Override
			//public Iterable<Tuple2<String,Double>> call(Iterable<String> it)
			public Iterable<Tuple2<Double,String>> call(Iterable<String> it)
			{
				List<Tuple2<Double,String>> results = new ArrayList<Tuple2<Double, String>>();
				//List<Double> distance=new ArrayList<Double>();
				int k=5;
				double radius=0.0;
				int counter=-1;
				//List<Tuple2<String,Double>> results = new ArrayList<Tuple2<String, Double>>();
				Iterator<String> ir=it.iterator();//原来还有这个玩意啊  import org.apache.commons.math3.util.MultidimensionalCounter.Iterator;
				String[] tmp=null;
				String[] tmp_vector_0=null;
				String[] tmp_vector_1=null;
				double sim_vector=0.0;
				int length_of_vector=0;
				double max=0.0;
				int j=0;
				//if(ir!=null)
				//{
				while(ir.hasNext()&&j<k)
				{
					tmp=ir.next().toString().split("\\|");
					tmp_vector_0=tmp[0].split("\\,");
					tmp_vector_1=tmp[1].split("\\,");
					length_of_vector=tmp_vector_0.length;
					sim_vector=0.0;
					for(int i=0;i<length_of_vector;i++)
					{
						sim_vector+=Math.pow(Double.valueOf(tmp_vector_0[i])-Double.valueOf(tmp_vector_1[i]),2);
						
						//System.out.println(tmp_vector_0[i]+tmp_vector_1[i]);
					}
					//System.out.println(sim_vector);
					
					
					if(max<sim_vector)
					{
						max=sim_vector;
						
						counter=j;
						
						radius=Math.sqrt(max);
					}
					results.add(new Tuple2<Double,String>(sim_vector,tmp[0]+"|"+tmp[1]));
					
					j++;
					
					//counter++;
					//results.add(new Tuple2<String,Double>(tmp[0]+"|"+tmp[1],sim_vector));
				}
				//}
				while(ir.hasNext())//here we could make our improved knn come true
				{
					tmp=ir.next().toString().split("\\|");
					tmp_vector_0=tmp[0].split("\\,");
					tmp_vector_1=tmp[1].split("\\,");
					sim_vector=0.0;
					boolean biggerornot=false;
					for(int i=0;i<length_of_vector;i++)
					{
						if((Double.valueOf(tmp_vector_0[i])-Double.valueOf(tmp_vector_1[i])>radius)||(Double.valueOf(tmp_vector_1[i])-Double.valueOf(tmp_vector_0[i])>radius))
						{
							biggerornot=true;
							break;
						}
					}
					
					/*for(int i=0;i<length_of_vector;i++)
					{
						sim_vector+=Math.pow(Double.valueOf(tmp_vector_0[i])-Double.valueOf(tmp_vector_1[i]),2);
						//System.out.println(tmp_vector_0[i]+tmp_vector_1[i]);
					}*/	
					//System.out.println(sim_vector);
					if(!biggerornot){
						
						for(int i=0;i<length_of_vector;i++)
						{
							sim_vector+=Math.pow(Double.valueOf(tmp_vector_0[i])-Double.valueOf(tmp_vector_1[i]),2);
							//System.out.println(tmp_vector_0[i]+tmp_vector_1[i]);
						}	
						
						if(sim_vector<max)
						{
							results.remove(counter);
							results.add(new Tuple2<Double,String>(sim_vector,tmp[0]+"|"+tmp[1]));
							max=0.0;
							for(int p=0;p<k;p++)
							{
								if(results.get(p)._1>max)
								{
									max=results.get(p)._1;
									counter=p;
								}
							}
							radius=Math.sqrt(max);
						}
					}
					//results.add(new Tuple2<String,Double>(tmp[0]+"|"+tmp[1],sim_vector));
				}
				return results;
			}
		});//.sortByKey();
		
		sim.saveAsTextFile("hdfs://master:8020/user/hdfs/lpp/result");
		
		/*JavaPairRDD<Double,String> res_27=sim.groupBy(new Function<Tuple2<Double,String>,Tuple2<Double,String>>(){
			@Override
			public Tuple2<Double,String> call(Tuple2<Double,String> t)
			{
				
				//<Tuple2<Double,String>,Iterable<Tuple2<Double,String>>>
				
				
				List<Tuple2<Double,String>> res= new ArrayList<Tuple2<Double, String>>();
				return new Tuple2<Double,String>(t._1,t._2);
			}
			});*/
		/*List<Tuple2<Double,String>> re=sim.collect();
		for(Tuple2<Double,String> t_t:re)
		{
			System.out.println(t_t._1() + "\t" + t_t._2());
		}*/
		
		
	}

}


