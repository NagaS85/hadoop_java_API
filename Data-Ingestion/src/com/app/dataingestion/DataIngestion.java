package com.app.dataingestion;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
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
import java.util.concurrent.locks.ReentrantLock;
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
	//private static FileWriter fstream = null;
	//private static BufferedWriter out = null;
	private static LoadProperties props = LoadProperties.getInstance();
	//final static Logger logger = Logger.getLogger(DataIngestion.class);
	private static ReentrantLock l = new ReentrantLock();
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
		//Set<String> booknames = getBookNames(args[0]);
		Set<String> fileNames = getUnProcessedFileNames(args[0]);
		//Set<Future<Set<FileMetaData>>> resultSet = new HashSet<>();
		Set<Future<String>> list = new HashSet<Future<String>>();
		//Creating CyclicBarrier with number of file names threads.
        final CyclicBarrier cb = new CyclicBarrier(fileNames.size(), new Runnable(){
        	//This task will be executed once all thread reaches barrier
        	@Override
            public void run(){
                
                System.out.println("All Threads are arrived at barrier");
                //
                zipArchiveFiles(args[0],args[1],args[2]);
               
                
            }
        });

		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
		
		
		fileNames.forEach(new Consumer<String>() {

			@Override
			public void accept(String fileName)  {
				DataIngestionCallable callable = new DataIngestionCallable(args[0],fileName,args[1],cb,l,args[2]);
				Future<String> future = executor.submit(callable);
				list.add(future);
			}
		});
		for(Future<String> f : list){
            try {
                //return value of Future, delay output in console
                // because Future.get() waits for task to get completed.
            	setMergeFileName(f.get());
            	//mergeFileName = f.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }  
		 System.out.println("mergeFileName.............."+getMergeFileName());
		 File mergeFile = new File(getMergeFileName());
		 List<File> fileList = splitFile(mergeFile, 1024,args[1]);
		 long split_files_Size=0;
		 for (File file : fileList) {
			 split_files_Size +=split_files_Size+file.length();
			System.out.println("file created...."+file.getCanonicalPath());
		 }
		 //System.out.println("split_files_Size.............."+split_files_Size);
		 //System.out.println("mergeFile size.............."+mergeFile.length());
		 if(mergeFile.length()==split_files_Size)
			 mergeFile.delete();
		 
		executor.shutdown();
		//metaDataFileWrite(resultSet,args[1]);
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
 * To get the file names from list of files in specified location(i.e small file location)
 * 
 * @param inputfolderPath
 * @return set of book names
 */
public static Set<String> getUnProcessedFileNames(String inputfolderPath)
{
	Set<String> fileNamesSet = new HashSet<>();
	File files = new File(inputfolderPath);
	if(new File(inputfolderPath).isDirectory())
	{
		for (File file : files.listFiles()) {
			if (file.isFile()) {
					fileNamesSet.add(file.getName());
			}
		}
	}

	return fileNamesSet;
	
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
							FileWriter fstream = new FileWriter(outfile, true);
							BufferedWriter out = new BufferedWriter(fstream);
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
	    			  System.out.println("file in writeFilesToHdfs..."+file.getName());
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
	 * @param inputDir
	 * @param outputDir
	 * @param fileType
	 */
	private static void zipArchiveFiles(String inputDir,String outputDir,String fileType) {
		try {
			
			Set<FileMetaData> metaDataSet =  FileTypeFactory.valueOf(fileType).getFileOperation().readMetaDataFile(outputDir);
			 for (FileMetaData fileMetaData : metaDataSet) {
				System.out.println("fileMetaData fileName........"+fileMetaData.getFileName());
			}
			// now zip files one by one
			// create ZipOutputStream to write to the zip file
			 
			
			File files = new File(inputDir);
			if(new File(inputDir).isDirectory())
			{
				
				
				for (File file : files.listFiles()) {
					
					Optional<FileMetaData> result = metaDataSet.stream().parallel().filter(n->n.getFileName().equalsIgnoreCase(file.getName())).findAny();
					System.out.println("result......"+result.isPresent());
					if(result.isPresent())
					{
						FileOutputStream fos = //new File
								new FileOutputStream(props.getValue("ARCHIVE_FOLDER_PATH")+"/"+file.getName()+".zip",true);
						ZipOutputStream zos = new ZipOutputStream(fos);
							System.out.println("Zipping " + file.getName());
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
							
							file.delete();//delete file from inputDir after processing.
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
    		
    			




