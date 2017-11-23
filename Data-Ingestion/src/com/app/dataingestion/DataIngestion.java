package com.app.dataingestion;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.app.dataingestion.model.FileMetaData;
import com.app.dataingestion.util.LoadProperties;


/**
 * @author naga
 *
 */
public class DataIngestion {
	//private static final String INPUT_PATH =  "/usr/local/smallfiles/input";
	//private static final String OUTPUT_PATH =  "/usr/local/smallfiles/output";
	/*
	 * args[0]= input source path
	 * args[1]= output  path
	 * args[2]= FileType(Ex:CSV or JSON or TXT etc)
	 */
	private static FileWriter fstream = null;
	private static BufferedWriter out = null;
	private static LoadProperties props = LoadProperties.getInstance();
	final static Logger logger = Logger.getLogger(DataIngestion.class);
	private static ReentrantLock l = new ReentrantLock();
	
	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		
		//List of workbooks
		Set<String> booknames = getBookNames(args[0]);
		//Creating CyclicBarrier with number of booking names threads.
        final CyclicBarrier cb = new CyclicBarrier(booknames.size(), new Runnable(){
        	//This task will be executed once all thread reaches barrier
        	@Override
            public void run(){
                
                System.out.println("All Threads are arrived at barrier");
                //TODO-Logic to move processed files from source location to other folder
            }
        });

		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
		Set<Future<Set<FileMetaData>>> resultSet = new HashSet<>();
		
		booknames.forEach(new Consumer<String>() {

			@Override
			public void accept(String t)  {
				DataIngestionCallable callable = new DataIngestionCallable(args[0],t,args[1],cb,l,args[2]);
				Future<Set<FileMetaData>> result = executor.submit(callable);
				resultSet.add(result);
			}
		});
	
		metaDataFileWrite(resultSet,args[1]);
		writeFilesToHdfs(args[1]);
		
}

/**
 * To get the workbook names from list of files in specified location(i.e small file location)
 * Filenames should follow the naming pattern like {bookName}_XXXXX.csv
 * 
 * @param inputfolderPath
 * @return set of book names
 */
public static Set<String> getBookNames(String inputfolderPath)
{
	Set<String> booksSet = new HashSet<>();
	File files = new File(inputfolderPath);
	if(new File(inputfolderPath).isDirectory())
	{
		for (File file : files.listFiles()) {
			if (file.isFile()) {
				String[] parts = file.getName().split("\\_");
				if(parts.length>0)
					booksSet.add(parts[0]);// Text before the first underscore
			}
		}
	}
	return booksSet;
	
}


/**
 * Prepare the meta data file which contains name of the files from list of processed small files
 * 
 * @param metadataList
 * @param outputPath
 */
public static void metaDataFileWrite(Set<Future<Set<FileMetaData>>> metadataList,String outputPath)
{
	try {
		File outfile = new File(outputPath+"/"+props.getValue("META_DATA_FILE_NAME"));
		metadataList.forEach( new Consumer<Future<Set<FileMetaData>>>() {
			
		@Override
		public void accept(Future<Set<FileMetaData>> t) {
			try {
				t.get().forEach( new Consumer<FileMetaData>() {
					@Override
					public void accept(FileMetaData t) {
						try {
							fstream = new FileWriter(outfile, true);
							out = new BufferedWriter(fstream);
							out.write(t.getFileName());
							out.newLine();
							out.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
						
					}
					
				});
				
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
	});
		
	} catch (Exception e) {
		e.printStackTrace();
	}
}

/**
 * write files to hdfs
 * @param localPath
 */
public static void writeFilesToHdfs(String localPath)
{
	System.out.println("Enter into writeFilesToHdfs....");
	Configuration conf = new Configuration();
	String hdfsPath=props.getValue("HDFS_FOLDER_PATH");
	File files = new File(localPath);
	int counter=0;
	try {
		FileSystem hdfs = FileSystem.get(URI.create(props.getValue("HDFS_URL")), conf);
		 //==== Create folder if not exists
	      Path newFolderPath= new Path(hdfsPath);
	      if(!hdfs.exists(newFolderPath)) {
	         // Create new Directory
	    	  hdfs.mkdirs(newFolderPath);
	    	  System.out.println("Path "+hdfsPath+" created.");
    		  for (File file : files.listFiles()) {
    			  hdfs.copyFromLocalFile(new Path(localPath+"/"+file.getName()), new Path(hdfsPath));
    			  counter = counter+1;
    		  }
	      }
	      if(hdfs.exists(newFolderPath)) {
	    	  if(files.isDirectory())
	  		{
	    		  System.out.println(" HDFS Path already available ...");
	    		  for (File file : files.listFiles()) {
	    			  hdfs.copyFromLocalFile(new Path(localPath+"/"+file.getName()), new Path(hdfsPath));
	    			  counter = counter+1;
	    		  }
	  		}
		   }
	      if(files.listFiles().length==counter){
	    	  System.out.println("No of files copied form local to HDFS:"+counter);
	      }
		
	} catch (IOException e) {
		e.printStackTrace();
	} 
}

}
