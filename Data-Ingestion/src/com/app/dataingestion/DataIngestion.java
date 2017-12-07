package com.app.dataingestion;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.app.dataingestion.model.FileMetaData;
import com.app.dataingestion.service.FileTypeFactory;
import com.app.dataingestion.util.LoadProperties;
import org.slf4j.*;


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
	private static LoadProperties props = LoadProperties.getInstance();
	final static Logger logger = LoggerFactory.getLogger(DataIngestion.class);
	//private static ReentrantLock l = new ReentrantLock();
	private static String mergeFileName;
	
	
	
	

	/**
	 * @return the mergeFileName
	 */
	public static String getMergeFileName() {
		return mergeFileName;
	}

	/**
	 * @param mergeFileName the mergeFileName to set
	 */
	public static void setMergeFileName(String mergeFileName) {
		DataIngestion.mergeFileName = mergeFileName;
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		
		//List of workbooks
		Set<String> fileNames = getUnProcessedFileNames(args[0],args[2]);
		
		//System.out.println("Un Processed Files:"+fileNames.size()+":::"+(fileNames.size()!=0 ? fileNames.size():"Exit"));
		logger.debug("Un Processed Files:"+fileNames.size()+":::"+(fileNames.size()!=0 ? fileNames.size():"Exit"));
		Set<Future<String>> list = new HashSet<Future<String>>();
		//Creating CyclicBarrier with number of file names threads.
		if(fileNames.size()==0)
		{
			logger.debug("There are no files to process::Exit");
			System.exit(0);
		}
			
		
        final CyclicBarrier cb = new CyclicBarrier(fileNames.size(), new Runnable(){
        	//This task will be executed once all thread reaches barrier
        	@Override
            public void run(){
                
        		logger.debug("All Threads are arrived at barrier");
                //
                zipArchiveFiles(args[0],args[2]);
               
                
            }
        });

		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
		
		
		fileNames.forEach(new Consumer<String>() {

			@Override
			public void accept(String fileName)  {
				DataIngestionCallable callable = new DataIngestionCallable(
						args[0], fileName, args[1], cb, 
						//l, 
						args[2]);
				Future<String> future = executor.submit(callable);
				list.add(future);
			}
		});
		for(Future<String> f : list){
            try {
                //return value of Future, delay output in console
                // because Future.get() waits for task to get completed.
            	setMergeFileName(f.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }  
		 if(getMergeFileName()!=null)
		 {
			 File mergeFile = new File(getMergeFileName());
			 List<File> fileList = splitFile(mergeFile, 1024,args[1]);
			 long split_files_Size=0;
			 for (File file : fileList) {
				 split_files_Size +=split_files_Size+file.length();
			 }
			 if(mergeFile.length()==split_files_Size)
				 mergeFile.delete();
			 writeFilesToHdfs(args[1]);
		 }
		executor.shutdown();
		
		
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
 * To get the file names from list of files in specified location(i.e small file location)
 * 
 * @param inputfolderPath
 * @return set of book names
 */
public static Set<String> getUnProcessedFileNames(String inputDir,String fileType)
{
	Set<String> fileNamesSet = new HashSet<>();
	//Read already  processed files to filter unprocessed files from input Dir.
	Set<FileMetaData> metaDataSet =  FileTypeFactory.valueOf(fileType).getFileOperation().readMetaDataFile(inputDir);
	
	File files = new File(inputDir);
	if(files.isDirectory())
	{
		for (File file : files.listFiles()) {
			//Result return true if file is already processed.
			Optional<FileMetaData> result = metaDataSet
					.stream()
					.parallel()
					.filter(n -> n.getFileName().equalsIgnoreCase(
							file.getName())).findAny();
			
			// Need not consider metadata_flatfile.csv 	
			 if (file.isFile()
						&& (!props.getValue("META_DATA_FILE_NAME")
								.equalsIgnoreCase(file.getName()))
						&& result.isPresent() == false) {
					fileNamesSet.add(file.getName());
				}
		}
	}

	return fileNamesSet;
	
}



/**
 * write files to hdfs
 * @param dir-directory where merge files available
 */
public static void writeFilesToHdfs(String dir)
{
	Configuration conf = new Configuration();
	String hdfsPath=props.getValue("HDFS_FOLDER_PATH");
	File files = new File(dir);
	int counter=0;
	try {
		FileSystem hdfs = FileSystem.get(URI.create(props.getValue("HDFS_URL")), conf);
		 //==== Create folder if not exists
	      Path newFolderPath= new Path(hdfsPath);
	      if(!hdfs.exists(newFolderPath)) {
	         // Create new Directory
	    	  hdfs.mkdirs(newFolderPath);
	    	  logger.debug("Path "+hdfsPath+" created.");
    		  for (File file : files.listFiles()) {
    			  //hdfs.copyFromLocalFile(new Path(dir+"/"+file.getName()), new Path(hdfsPath));
    			  hdfs.moveFromLocalFile(new Path(dir+"/"+file.getName()), new Path(hdfsPath));
    			  counter = counter+1;
    		  }
	      }
	      if(hdfs.exists(newFolderPath)) {
	    	  if(files.isDirectory())
	  		{
	    		  logger.debug(" HDFS Path already available ...");
	    		  for (File file : files.listFiles()) {
	    			  logger.debug("Written file in HdFS::"+file.getName());
	    			  //hdfs.copyFromLocalFile(new Path(dir+"/"+file.getName()), new Path(hdfsPath));
	    			  hdfs.moveFromLocalFile(new Path(dir+"/"+file.getName()), new Path(hdfsPath));
	    			  counter = counter+1;
	    		  }
	  		}
		   }
	      if(files.listFiles().length==counter){
	    	  logger.debug("No of files copied form local to HDFS:"+counter);
	    	  
	      }
		
	} catch (IOException e) {
		e.printStackTrace();
	} 
}

/**
 * @param file
 * @param sizeOfFileInMB
 * @return list of split files
 * @throws IOException
 */
public static List<File> splitFile(File file, int sizeOfFileInMB,String outPutPath) throws IOException {
    int counter = 1;
    List<File> files = new ArrayList<File>();
    int sizeOfChunk = 1024 * 1024 * sizeOfFileInMB;
    String eof = System.lineSeparator();
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
        String line = br.readLine();
        while (line != null) {
				File newFile = new File(outPutPath+"/"+FilenameUtils.removeExtension(file
						.getName())
						+ "_"
						+ String.format("%03d", counter++)
						+ ".csv");
            try (OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile))) {
                int fileSize = 0;
                while (line != null) {
                    byte[] bytes = (line + eof).getBytes(Charset.defaultCharset());
                    if (fileSize + bytes.length > sizeOfChunk)
                        break;
                    out.write(bytes);
                    fileSize += bytes.length;
                    line = br.readLine();
                }
            }
            files.add(newFile);
        }
    }
    return files;
}

	/**
	 * @param inputDir-Directory where small files available
	 * @param outputDir-Merge files location
	 * @param fileType-Type of file
	 */
	private static void zipArchiveFiles(String inputDir,String fileType) {
		try {
			
			Set<FileMetaData> metaDataSet =  FileTypeFactory.valueOf(fileType).getFileOperation().readMetaDataFile(inputDir);
			// now zip files one by one
			// create ZipOutputStream to write to the zip file
			File files = new File(inputDir);
			if(new File(inputDir).isDirectory())
			{
				for (File file : files.listFiles()) {
					
					Optional<FileMetaData> result = metaDataSet.stream().parallel().filter(n->n.getFileName().equalsIgnoreCase(file.getName())).findAny();
					if(result.isPresent())
					{
						FileOutputStream fos = //new File
								new FileOutputStream(props.getValue("ARCHIVE_FOLDER_PATH")+"/"+file.getName()+".zip",true);
							ZipOutputStream zos = new ZipOutputStream(fos);
							logger.debug("Zipping " + file.getName());
							// for ZipEntry we need to keep only relative file path, so we
							// used substring on absolute path
							ZipEntry ze = new ZipEntry(file.getName());
							zos.putNextEntry(ze);
							// read the file and write to ZipOutputStream
							FileInputStream fis = new FileInputStream(file);
							byte[] buffer = new byte[1024];
							int len;
							while ((len = fis.read(buffer)) > 0) {
								zos.write(buffer, 0, len);
							}
							zos.closeEntry();
							fis.close();
							zos.close();
							fos.close();
					}	
						
					}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
    		
    			




