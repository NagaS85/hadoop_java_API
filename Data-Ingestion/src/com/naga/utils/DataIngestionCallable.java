package com.naga.utils;
/**
 * @author naga
 *
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class DataIngestionCallable implements Callable<Set<FileMetaData>> {

	final static Logger logger = Logger.getLogger(DataIngestionCallable.class);
	String bookName=null;
	FileOutputStream outstream = null;
	FileInputStream instream = null;
	String outpath=null;
	String inPath=null;
	Set<FileMetaData> set=null;
	String processedPath=null;
	private static LoadProperties props = LoadProperties.getInstance();
	private CyclicBarrier barrier;
	private static ReentrantLock l;
	public DataIngestionCallable(String inPath, String bookName,String outpath,CyclicBarrier barrier,ReentrantLock l) {
		this.inPath = inPath;
		this.bookName = bookName;
		this.outpath = outpath;
		this.barrier = barrier;
		DataIngestionCallable.l=l;
		
	}
	
	@Override
	public Set<FileMetaData> call() throws Exception {
		try {
			set = mergeFiles(inPath,bookName,outpath);
			l.unlock();// unlock thread 
			System.out.println(Thread.currentThread().getName() + " is waiting on barrier");
			barrier.await();
			System.out.println(Thread.currentThread().getName() + " has crossed the barrier");
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch (InterruptedException | BrokenBarrierException ex) {
			
			ex.printStackTrace();
        }

	return set;
	}
	//Merge small files with respect to each workname.
	//csv files should contain the headers since header is removed while merging files.
	public Set<FileMetaData> mergeFiles(String inPath,String bookName,String outPath) throws IOException
	{
		
		l.lock(); // get lock to perform safe operations
		File files = new File(inPath);
		File outfile =new File(outPath+"/"+bookName+"_merge_"+LocalDate.now()+".csv");
		FileWriter fstream = null;
		BufferedWriter out = null;
		Configuration conf = new Configuration();
		Set<FileMetaData> metaDataSet =  readMetaDataFile(outPath);
		//System.out.println("metaDataSet size...."+metaDataSet.size());
		Set<FileMetaData> set1=null;
		if(files.isDirectory())
		{
			try {
				fstream = new FileWriter(outfile, true);
				out = new BufferedWriter(fstream);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			set1 = new HashSet<>();
			for (File file : files.listFiles()) {
				FileInputStream fis;
				List<FileMetaData> filterData = metaDataSet.stream()
						.filter(n -> file.getName().equals(n.getFileName()))
						.collect(Collectors.toList());
						
				
				if (file.isFile() && file.getName().contains(bookName)) {
					if(filterData.size()==1 && file.getName().equals(filterData.get(0).getFileName()))
					{
						System.out.println("File '"+file.getName()+"' already processed and merged to : "+outfile);
					}
					else{
						
						fis = new FileInputStream(file);
						BufferedReader in = new BufferedReader(new InputStreamReader(fis));
						String aLine;
						aLine = in.readLine();//skipping header from csv
						while ((aLine = in.readLine()) != null) {
							out.write(aLine);
							out.newLine();
						}
						in.close();
						FileMetaData fileMetaData = new FileMetaData();
						fileMetaData.setFileName(file.getName());	
						set1.add(fileMetaData);
					}
				}
				
			//if(file.delete())
				//System.out.println("File Deleted after ....."+file.getCanonicalPath());
		}
			out.close();
		}
		System.out.println("Completed...."+Thread.currentThread().getName());
		return set1;
		
	}
	//To get the workbook names from list of files in specified location(i.e small file location)
	//Filenames should follow the naming pattern like {bookName}_XXXXX.csv
	public static Set<FileMetaData> readMetaDataFile(String outPath)
	{
		Set<FileMetaData> processedFilenames = new HashSet<>();
		File file = new File(outPath+"/"+props.getValue("META_DATA_FILE_NAME"));
				if (file.isFile()) {
					FileInputStream fis;
					try {
						fis = new FileInputStream(file);
						BufferedReader bufferedReader =  new BufferedReader(new InputStreamReader(fis));
						String aLine;
						FileMetaData data = null;
						while ((aLine = bufferedReader.readLine()) != null) {
							data = new FileMetaData();
							data.setFileName(aLine);
							processedFilenames.add(data);
							}
						bufferedReader.close();
					} 
					catch (IOException e) {
						e.printStackTrace();
					}
					
					
				}
		
		return processedFilenames;
		
	}	


}
