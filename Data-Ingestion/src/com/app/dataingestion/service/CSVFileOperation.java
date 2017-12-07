/**
 * 
 */
package com.app.dataingestion.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.app.dataingestion.DataIngestionCallable;
import com.app.dataingestion.model.FileMetaData;
import com.app.dataingestion.util.LoadProperties;

/**
 * @author naga
 *
 */
public class CSVFileOperation implements FileOperation{
	
	private static LoadProperties props = LoadProperties.getInstance();
	final static Logger logger = LoggerFactory.getLogger(CSVFileOperation.class);
	
	
	/**
	 * Merge small files with respect to each workname.
	   csv files should contain the headers since header is removed while merging files.
	 */
	
	/* (non-Javadoc)
	 * @see com.naga.utils.FileOperation#mergeFiles(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public String mergeFiles(String inPath, String fileName,
			String outPath) throws IOException {
		
		
		File fileNamesLogfile = new File(inPath+"/"+props.getValue("META_DATA_FILE_NAME"));
		File outfile =new File(outPath+"/merge_"+new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date())+".csv");
		logger.debug("outPath........."+outPath);
		FileWriter fw =null;
		BufferedWriter buffer =null;
		Set<FileMetaData> metaDataSet =  readMetaDataFile(inPath);
		try {
			File files = new File(inPath);
			
			
			if(files.isDirectory())
			{
				
				
				for (File file : files.listFiles()) {
					FileWriter outFileWriter = new FileWriter(outfile, true);
					BufferedWriter outBuffer = new BufferedWriter(outFileWriter);
					Optional<FileMetaData> result = metaDataSet
							.stream()
							.parallel()
							.filter(n -> n.getFileName().equalsIgnoreCase(
									file.getName())).findAny();
					
					
					if (file.isFile() && file.getName().contains(fileName)) {
						
						if(result.isPresent()==true )
						{
							logger.debug("File '"+file.getName()+"' already processed ");
						}
						else{
						
						FileInputStream fis = new FileInputStream(file);
							BufferedReader in = new BufferedReader(new InputStreamReader(fis));
							String aLine;
							aLine = in.readLine();//skipping header from csv
							while ((aLine = in.readLine()) != null) {
								outBuffer.write(file.getName()+","+aLine);//Adding filename column to identify data 
								outBuffer.newLine();
							}
							in.close();
							fis.close();
							fw = new FileWriter(fileNamesLogfile, true);
							buffer = new BufferedWriter(fw);
							if(!props.getValue("META_DATA_FILE_NAME").equalsIgnoreCase(file.getName())) // need not to log metadata_flatfile.csv in same file since it is in local Dir.
							{
								buffer.write(file.getName());
								buffer.newLine();
							}
							buffer.close();
					}
					}		
					outBuffer.flush();
					outBuffer.close();
				
			}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.debug("Completed...."+Thread.currentThread().getName());
			return outfile.getCanonicalPath();
		
	
	}
	
	//To get the workbook names from list of files in specified location(i.e small file location)
	//Filenames should follow the naming pattern like {bookName}_XXXXX.csv
	/* (non-Javadoc)
	 * @see com.naga.utils.FileOperation#readMetaDataFile(java.lang.String)
	 */
	@Override
	public Set<FileMetaData> readMetaDataFile(String dir) {

		Set<FileMetaData> processedFilenames = new HashSet<>();
		File file = new File(dir+"/"+props.getValue("META_DATA_FILE_NAME"));
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
