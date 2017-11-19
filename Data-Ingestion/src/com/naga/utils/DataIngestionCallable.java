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
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;

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
	public DataIngestionCallable(String inPath, String bookName,String outpath,CyclicBarrier barrier) {
		this.inPath = inPath;
		this.bookName = bookName;
		this.outpath = outpath;
		this.barrier = barrier;
		
	}
	
	@Override
	public Set<FileMetaData> call() throws Exception {
		try {
			set = mergeFiles(inPath,bookName,outpath);
			logger.info(Thread.currentThread().getName() + " is waiting on barrier");
			barrier.await();
			logger.info(Thread.currentThread().getName() + " has crossed the barrier");
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch (InterruptedException ex) {
			logger.info(DataIngestionCallable.class.getName());
        } catch (BrokenBarrierException ex) {
        	logger.info(DataIngestionCallable.class.getName());
        }

	return set;
	}
	//Merge small files with respect to each workname.
	//csv files should contain the headers since header is removed while merging files.
	public Set<FileMetaData> mergeFiles(String inPath,String bookName,String outPath) throws IOException
	{
		
		
		File files = new File(inPath);
		File outfile =new File(outPath+"/"+bookName+"_merge_"+LocalDate.now()+".csv");
		FileWriter fstream = null;
		BufferedWriter out = null;
		Configuration conf = new Configuration();
		if(files.isDirectory())
		{
			try {
				fstream = new FileWriter(outfile, true);
				out = new BufferedWriter(fstream);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			set = new HashSet<>();
			for (File file : files.listFiles()) {
				FileInputStream fis;
				if (file.isFile() && file.getName().contains(bookName)) {
					fis = new FileInputStream(file);
					BufferedReader in = new BufferedReader(new InputStreamReader(fis));
					String aLine;
					aLine = in.readLine();//skipping header from csv
					while ((aLine = in.readLine()) != null) {
						out.write(aLine);
						out.newLine();
					}
					in.close();
					/*conf.setBoolean("dfs.support.append", true);
					InputStream in = new BufferedInputStream(new FileInputStream(file));
					//Destination file in HDFS
					FileSystem fileSystem = FileSystem.get(URI.create(props.getValue("HDFS_URL")), conf);
					OutputStream out1 = fileSystem.create(new Path(props.getValue("HDFS_FOLDER_PATH")+"/"+bookName+"_merge_"+LocalDate.now()+".csv"));
					Path hdfsFilePath = new Path(props.getValue("HDFS_FOLDER_PATH")+"/"+bookName+"_merge_"+LocalDate.now()+".csv");
					if (!fileSystem.exists(hdfsFilePath)) {
						fileSystem.create(hdfsFilePath);
				         System.out.println("Path "+hdfsFilePath+" created.");
				         fileSystem.close();
				    }
					else{
						Boolean isAppendable = Boolean.valueOf(fileSystem.getConf().get("dfs.support.append"));
						if(isAppendable) {
					        
							FileSystem fileSystem1 = FileSystem.get(URI.create(props.getValue("HDFS_URL")), conf);
							FSDataOutputStream fs_append = fileSystem1.append(hdfsFilePath);
					       
					        int n;
					        byte[] buffer = new byte[1024];
					        while((n = in.read(buffer)) > -1) {
					        	fs_append.write(buffer, 0, n);   // Don't allow any extra bytes to creep in, final write
					        }
					        
					    }
					    else {
					        System.err.println("Please set the dfs.support.append property to true");
					        
					    }
					}
					//Copy file from local to HDFS
					//IOUtils.copyBytes(in, out1, 4096, true);
					*/
					FileMetaData fileMetaData = new FileMetaData();
					fileMetaData.setFileName(file.getName());	
					set.add(fileMetaData);
				}
				
			//if(file.delete())
				//System.out.println("File Deleted after ....."+file.getCanonicalPath());
		}
			out.close();
		}
		logger.info("Completed...."+Thread.currentThread().getName());
		return set;
	}
	//To get the workbook names from list of files in specified location(i.e small file location)
	//Filenames should follow the naming pattern like {bookName}_XXXXX.csv
	public static Set<String> readMetaDataFile(String outPath)
	{
		Set<String> processedFilenames = new HashSet<>();
		File file = new File(outPath+"/"+props.getValue("META_DATA_FILE_NAME"));
				if (file.isFile()) {
					FileInputStream fis;
					try {
						fis = new FileInputStream(file);
						BufferedReader bufferedReader =  new BufferedReader(new InputStreamReader(fis));
						String aLine;
						while ((aLine = bufferedReader.readLine()) != null) {
								processedFilenames.add(aLine);
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
