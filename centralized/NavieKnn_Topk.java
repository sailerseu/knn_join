package spark_java.knn;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.math.*;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;

public class NavieKnn_Topk {
	//private static String path="D:/tmp_tokenizer/twitter.txt"; 
	private static String path="D:/tmp_tokenizer/wine4500.xls"; 
	private static int k=5;
	public static void main(String[] args){

		//ArrayList<Topk> tt=navieknn2();	
		
		//ArrayList<Topk> ss=ballknn2();
		
		
		
		
		
		ArrayList<Topk> ss=ballknn();		
		for(int i=0;i<k;i++)
		{
			System.out.println(ss.get(i).distance+" "+ss.get(i).vector);
		}
		
		/*ArrayList<Topk> tt=navieknn();
		for(int i=0;i<k;i++)
		{
			System.out.println(tt.get(i).distance+"  "+tt.get(i).vector);
		}*/
		
		
	}
	
	public static ArrayList<Topk> navieknn()
	{
		
		ArrayList<ArrayList<Double>> vt_arr=new ArrayList<ArrayList<Double>>();//all the document vector
		//ArrayList<Double> vt=new ArrayList<Double>();//single document vector
		ArrayList<Topk> topk=new ArrayList<Topk>();
		if(path.endsWith("txt")||path.endsWith("trn"))
		{
			vt_arr=getallvectors();
		}else if(path.endsWith("xls"))
		{
			vt_arr=getallvectorsfromExcel();
		}
		long starttime=System.nanoTime();
		int lengthofallvt=0;
		int lengthofsinglevt=0;
		
		lengthofsinglevt=vt_arr.get(0).size();
		lengthofallvt=vt_arr.size();
		ArrayList<Double> dest=new ArrayList<Double>();
		dest=vt_arr.get(0);
		
		//vt_arr.remove(0);//do not remove any elements,because removing takes too much time 
		for(int pre=1;pre<k+1;pre++)//computing distances of first k elements
		{
			double result=0.0;
			for(int p=0;p<lengthofsinglevt;p++)
			{
				result=result+Math.pow((dest.get(p)-vt_arr.get(pre).get(p)),2);
			}
			Topk top=new Topk(result, vt_arr.get(pre));
			topk.add(top);
			top=null;
			result=0.0;
		}
		
		//long st=System.nanoTime();
		for(int j=k+1;j<lengthofallvt;j++)
		{
			double result=0.0;
			for(int p=0;p<lengthofsinglevt;p++)
			{
				result=result+Math.pow((dest.get(p)-vt_arr.get(j).get(p)),2);
			}
			/*if(topk.size()==k)//if first top k is enough,next time we must find the nearer one to take the place
			{*/
				double max=0.0;
				int count=-1;
				
				for(int i=0;i<k;i++)
				{
					if(topk.get(i).distance>max)
					{
						max=topk.get(i).distance;
						count=i;
					}
				}
				if(max>result)//> or >=
				{
					
					topk.remove(count);
					Topk top=new Topk(result,vt_arr.get(j));
					topk.add(top);
					top=null;
					result=0.0;
				}
			 //}
				/*else{
				Topk top=new Topk(result, vt_arr.get(j));
				topk.add(top);
				top=null;
				result=0.0;
			}*/
		}
		//System.out.println("第二个循环"+(System.nanoTime()-st));
		System.out.println("navieknn time takes :"+(System.nanoTime()-starttime));
		return topk;
	}
	
	public static ArrayList<Topk> navieknn2()
	{
		ArrayList<ArrayList<Double>> allvectors=null;
		if(path.endsWith("txt")||path.endsWith("trn"))
		{
			allvectors=getallvectors();
		}else if(path.endsWith("xls"))
		{
			allvectors=getallvectorsfromExcel();
		}
		long start=System.nanoTime();
		ArrayList<Topk> topk=new ArrayList<Topk>();
		ArrayList<Double> dest=new ArrayList<Double>();
		ArrayList<Double> vector=new ArrayList<Double>();
		dest=allvectors.get(0);
		allvectors.remove(0);
		int lengthofsinglevector=dest.size();
		
		while(!allvectors.isEmpty())
		{
			vector=allvectors.get(0);
			allvectors.remove(0);
			double tmp=0;
			for(int i=0;i<lengthofsinglevector;i++)
			{
				tmp+=Math.pow((dest.get(i)-vector.get(i)),2);
			}
			double max=0;
			if(topk.size()==k)
			{
				int count=-1;
				for(int i=0;i<k;i++)
				{
					if(topk.get(i).distance>max)
					{
						max=topk.get(i).distance;
						count=i;
					}
				}
				
				if(max>tmp)
				{
					/*topk.remove(max);
					topk.put(tmp,vector);*/
					
					Topk top=new Topk(tmp,vector);
					topk.add(top);
					topk.remove(count);
					max=0;
					vector=null;
				}
			}else{
				//topk.put(tmp, vector);
				
				Topk top=new Topk(tmp,vector);
				topk.add(top);
				vector=null;
				tmp=0;
				//System.out.println("the knnnavie2 result:"+topk.size());
			}
			
		}
		System.out.println("计算时间navie2： "+(System.nanoTime()-start));
		//System.out.println("topk size is "+topk.size());
		return topk;
	}
	
	public static ArrayList<Topk> ballknn()
	{
		ArrayList<ArrayList<Double>> vt_arr=new ArrayList<ArrayList<Double>>();//all the document vector
		ArrayList<Topk> topk=new ArrayList<Topk>();
		if(path.endsWith("txt"))
		{
			vt_arr=getallvectors();
		}else if(path.endsWith("xls"))
		{
			vt_arr=getallvectorsfromExcel();
		}
		long starttime=System.nanoTime();
		int lengthofallvt=0;
		int lengthofsinglevt=vt_arr.get(0).size();
		lengthofallvt=vt_arr.size();
		ArrayList<Double> dest=new ArrayList<Double>();
		dest=vt_arr.get(0);
		
		//System.out.println("dest="+dest);
		
		//vt_arr.remove(0);
		//System.out.println("lengthofsinglevt:"+lengthofsinglevt);
		int co=0;
		double radius=0.0;//the radius of the largest circle
		double max=0.0;
		int count=-1;
		
		for(int pre=1;pre<k+1;pre++)//computing distances of first k elements
		{
			double result=0.0;
			for(int p=0;p<lengthofsinglevt;p++)
			{
				result=result+Math.pow((dest.get(p)-vt_arr.get(pre).get(p)),2);
			}
			Topk top=new Topk(result, vt_arr.get(pre));
			topk.add(top);
			top=null;
			result=0.0;
		}
		for(int i=0;i<k;i++)
		{
			if(topk.get(i).distance>max)
			{
				max=topk.get(i).distance;
				count=i;
			}
		}
		
		radius=Math.sqrt(max);
		
		for (int j = k+1; j < lengthofallvt; j++) {//first element is removed as a destination vector
			boolean biggerornot=false;
			double result=0.0;
			/*if (topk.size() == k)// if first top k is enough,next time we leave out the one whose partial is bigger than the largest one
			{*/
					for(int p=0;p<lengthofsinglevt;p++)//if any one of the all dimensions is bigger than the radius,just jump over 
						//for(int p=0;p<100;p++)
					{
						if(radius<(dest.get(p)-vt_arr.get(j).get(p))||radius<(vt_arr.get(j).get(p)-dest.get(p)))
						{
							biggerornot=true;
							break;
						}
					}
					if(!biggerornot){
						for(int p=0;p<lengthofsinglevt;p++)
							//for(int p=0;p<100;p++)
						{
							result=result+Math.pow((dest.get(p)-vt_arr.get(j).get(p)),2);
						}
						
						if (max > result) {
							topk.remove(count);
							Topk top=new Topk(result,vt_arr.get(j));
							topk.add(top);
							max=0.0;
							for(int i=0;i<k;i++)//get the max one
							{
								if(topk.get(i).distance>max)
								{
									max=topk.get(i).distance;
									count=i;
								}
							}
							
							radius=Math.sqrt(max);
							
						}
				
					}else{
						co++;
					}
					
			//}
			/*else {
				for(int p=0;p<lengthofsinglevt;p++)
				{
					result=result+Math.pow(dest.get(p)-vt_arr.get(j).get(p),2);
				}
				Topk top=new Topk(result, vt_arr.get(j));
				topk.add(top);
				result = 0.0;
				
				if(topk.size() == k){
					for(int i=0;i<k;i++)
					{
						if(topk.get(i).distance>max)
						{
							max=topk.get(i).distance;
							count=i;
						}
					}
					radius=Math.sqrt(max);
				}
			}*/
		}
		
		System.out.println("ballknn take times:"+(System.nanoTime()-starttime));
		System.out.println("co=="+co);
		return topk;
	}
	
	public static ArrayList<Topk> ballknn2()
	{
		ArrayList<ArrayList<Double>> allvectors=null;
		if(path.endsWith("txt")||path.endsWith("trn"))
		{
			allvectors=getallvectors();
		}
		if(path.endsWith("xls"))
		{
			allvectors=getallvectorsfromExcel();
		}
		long start=System.nanoTime();
		ArrayList<Topk> topk=new ArrayList<Topk>();
		ArrayList<Double> dest=new ArrayList<Double>();
		ArrayList<Double> vector=new ArrayList<Double>();
		dest=allvectors.get(0);
		allvectors.remove(0);
		int lengthofsinglevector=dest.size();
		double radius=0.0;
		double max = 0.0;
		int co=0;
		double tmp_max=0.0;
		int count =-1;
		while(!allvectors.isEmpty())
		{
			boolean biggerornot=false;
			double result=0.0;
			vector=allvectors.get(0);
			allvectors.remove(0);
			if(topk.size()==k){
				
				for(int i=0;i<lengthofsinglevector;i++)
				{
					if(radius<(dest.get(i)-vector.get(i))||radius<(vector.get(i)-dest.get(i)))
					{
						biggerornot=true;
						break;
					}
				}
				if(!biggerornot){
					for(int p=0;p<lengthofsinglevector;p++)
						//for(int p=0;p<100;p++)
					{
						result=result+Math.pow((dest.get(p)-vector.get(p)),2);
						//result=result+(dest.get(p)-vector.get(p))*(dest.get(p)-vector.get(p));
					}
					if (max > result) {
						max=0.0;
						topk.remove(count);
						Topk top=new Topk(result,vector);
						topk.add(top);
						for(int i=0;i<k;i++)//get the max one
						{
							if(topk.get(i).distance>max)
							{
								max=topk.get(i).distance;
								count=i;
							}
						}
						radius=Math.sqrt(max);
					}
				}else{
					co++;
				}
			}else{
				for(int p=0;p<lengthofsinglevector;p++)
				{
					result=result+Math.pow(dest.get(p)-vector.get(p),2);
				}
				Topk top=new Topk(result,vector);
				topk.add(top);
				result = 0.0;
				if(topk.size() == k){
					for(int i=0;i<k;i++)
					{
						if(topk.get(i).distance>max)
						{
							max=topk.get(i).distance;
							count=i;
						}
					}
					radius=Math.sqrt(max);
				}
			}
		}
		System.out.println("ball time takes  ： "+(System.nanoTime()-start));
		//System.out.println("跳过了多少个点 ："+co);
		return topk;
	}
	public static ArrayList<ArrayList<Double>> getallvectors()//load all the document vectors into memory
	{
		ArrayList<ArrayList<Double>> allvectors=new ArrayList<ArrayList<Double>>();
		try{
			FileReader fr = new FileReader(path);
			
			BufferedReader br = new BufferedReader(fr);
			String line=null;
		
			while ((line = br.readLine()) != null) {
				ArrayList<Double> vt=new ArrayList<Double>();//single document vector
				String[] vt_str=line.split(",");
				//String[] vt_str=line.split("	");
				int lengthofsinglevt=vt_str.length;
				//System.out.println("lengthofsinglevt="+lengthofsinglevt);
				//for(int i=0;i<lengthofsinglevt-1;i++)//last column is the class label
				for(int i=0;i<lengthofsinglevt;i++)
				{
					vt.add(Double.parseDouble(vt_str[i]));
				}
				allvectors.add(vt);
			}
		}catch(IOException e)
		{
			e.printStackTrace();
		}
		//System.out.println("allvectors:"+allvectors.size());
		return allvectors;
	}
	public static ArrayList<ArrayList<Double>> getallvectorsfromExcel()
	{
		ArrayList<ArrayList<Double>> allvectors=new ArrayList<ArrayList<Double>>();
		POIFSFileSystem fs;
		HSSFWorkbook wb;
		HSSFSheet sheet;
		HSSFRow row;
		int rows;
		
		
		/*FileOutputStream out = null;  
		try{
		out = new FileOutputStream(new File("D:/tmp_tokenizer/wine4500.txt"));
		}catch(IOException e)
		{
			e.printStackTrace();
		}*/
		
		
		
		//fs = new POIFSFileSystem(is); 
		//wb = new HSSFWorkbook(fs);
		try{
			wb = new HSSFWorkbook(new FileInputStream(path));   
			sheet = wb.getSheet("winequality-white");//edit the sheet name
			rows = sheet.getPhysicalNumberOfRows();
			int lengthofvector=sheet.getRow(1).getCell(0).getStringCellValue().split(";").length;
			for(int i=1;i<rows;i++)
			{
				row=sheet.getRow(i);//first row is title,just skip
				Cell cell = row.getCell(0);
				String cell_string=cell.getStringCellValue();
				String [] cell_list=cell_string.split(";");
				ArrayList<Double> vt=new ArrayList<Double>();//single document vector
				//System.out.println("lengthofsinglevt="+lengthofvector);
				//for(int j=0;j<2;j++)
				for(int j=0;j<lengthofvector-1;j++)
				{
					vt.add(Double.parseDouble(cell_list[j])); 
					//System.out.print(Double.parseDouble(cell_list[j])+" ");
					//out.write((Double.parseDouble(cell_list[j])+" ").getBytes());
				}
				//out.write(("\n").getBytes());
				//System.out.println();
				allvectors.add(vt);
				 //System.out.println(cell.getStringCellValue());
				 //break;
			}
		}catch(IOException e)
		{
			e.printStackTrace();
		}
		//System.out.println("allvectors:"+allvectors.size());
		return allvectors;
	}
}
	