package com.naga.utils;
/**
 * @author naga
 *
 */
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
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class DataIngestion {
	//private static final String INPUT_PATH =  "/usr/local/smallfiles/input";
	//private static final String OUTPUT_PATH =  "/usr/local/smallfiles/output";
	private static FileWriter fstream = null;
	private static BufferedWriter out = null;
	private static LoadProperties props = LoadProperties.getInstance();
	final static Logger logger = Logger.getLogger(DataIngestion.class);
	
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		
		//List of workbooks
		Set<String> booknames = getBookNames(args[0]);
		//Creating CyclicBarrier with number of booking names threads.
        final CyclicBarrier cb = new CyclicBarrier(booknames.size(), new Runnable(){
            @Override
            public void run(){
                //This task will be executed once all thread reaches barrier
                logger.info("All Threads are arrived at barrier");
                File files = new File(args[0]);
                if(files.isDirectory())
        		{
                	for (File file : files.listFiles()) {
                		if(file.delete())
							try {
								logger.info("Deleted processed file from inputpath.."+file.getCanonicalPath());
							} catch (IOException e) {
								e.printStackTrace();
							}
                	}
        		}
            }
        });

		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
		Set<Future<Set<FileMetaData>>> resultSet = new HashSet<>();
		
		booknames.forEach(new Consumer<String>() {

			@Override
			public void accept(String t)  {
				DataIngestionCallable callable = new DataIngestionCallable(args[0],t,args[1],cb);
				Future<Set<FileMetaData>> result = executor.submit(callable);
				resultSet.add(result);
			}
		});
	
		metaDataFileWrite(resultSet,args[1]);
		writeFilesToHdfs(args[1]);
		
}
//To get the workbook names from list of files in specified location(i.e small file location)
//Filenames should follow the naming pattern like {bookName}_XXXXX.csv
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

//Prepare the metadata file which contains name of the files from list of processed small files
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
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				});
				out.close();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	});
		
	} catch (Exception e) {
		e.printStackTrace();
	}
}
public static void writeFilesToHdfs(String localPath)
{
	logger.info("Enter into writeFilesToHdfs....");
	Configuration conf = new Configuration();
	String hdfsPath=props.getValue("HDFS_FOLDER_PATH");
	logger.info("localPath...."+localPath);
	File files = new File(localPath);
	try {
		FileSystem hdfs = FileSystem.get(URI.create(props.getValue("HDFS_URL")), conf);
		 //==== Create folder if not exists
	      Path newFolderPath= new Path(hdfsPath);
	      logger.info("is Path contains...."+hdfs.exists(newFolderPath));
	      if(!hdfs.exists(newFolderPath)) {
	         // Create new Directory
	    	  hdfs.mkdirs(newFolderPath);
	    	  logger.info("Path "+hdfsPath+" created.");
	      }
	      if(hdfs.exists(newFolderPath)) {
	    	  if(files.isDirectory())
	  		{
	    		  logger.info("files.isDirectory()...."+files.isDirectory());
	    		  for (File file : files.listFiles()) {
	    			  logger.info("localPath...."+localPath+"/"+file.getName());
	    			  hdfs.copyFromLocalFile(new Path(localPath+"/"+file.getName()), new Path(hdfsPath));
	    		  }
	  		}
		   }
		
	} catch (IOException e) {
		e.printStackTrace();
	} 
}

}
