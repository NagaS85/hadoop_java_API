/**
 * 
 */
package com.app.dataingestion.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.app.dataingestion.model.FileMetaData;
import com.app.dataingestion.util.LoadProperties;

/**
 * @author naga
 *
 */
public class CSVFileOperation implements FileOperation{
	
	private static LoadProperties props = LoadProperties.getInstance();
	
	
	/**
	 * Merge small files with respect to each workname.
	   csv files should contain the headers since header is removed while merging files.
	 */
	
	/* (non-Javadoc)
	 * @see com.naga.utils.FileOperation#mergeFiles(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public Set<FileMetaData> mergeFilesByCategory(String inPath, String bookName,
			String outPath) throws IOException {
		
		
		
		//l.lock(); // get lock to perform safe operations
		File files = new File(inPath);
		File outfile =new File(outPath+"/"+bookName+"_merge_"+LocalDate.now()+".csv");
		FileWriter fstream = null;
		BufferedWriter out = null;
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
							out.write(file.getName()+","+aLine);//Adding filename column to identify data 
							out.newLine();
						}
						in.close();
						FileMetaData fileMetaData = new FileMetaData();
						fileMetaData.setFileName(outfile.getName());	
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
		
		
		Set<FileMetaData> set1 = new HashSet<>();
		File outMetafile = new File(outPath+"/"+props.getValue("META_DATA_FILE_NAME"));
		File outfile =new File(outPath+"/merge_"+LocalDate.now()+".csv");
		System.out.println("outPath........."+outPath);
		FileWriter fw =null;
		BufferedWriter buffer =null;
		try {
			File files = new File(inPath);
			
			
			if(files.isDirectory())
			{
				
				
				for (File file : files.listFiles()) {
					FileWriter outFileWriter = new FileWriter(outfile, true);
					BufferedWriter outBuffer = new BufferedWriter(outFileWriter);
					
					 
					if (file.isFile() && file.getName().contains(fileName)) {
						FileInputStream fis = new FileInputStream(file);
							BufferedReader in = new BufferedReader(new InputStreamReader(fis));
							String aLine;
							aLine = in.readLine();//skipping header from csv
							while ((aLine = in.readLine()) != null) {
								outBuffer.write(file.getName()+","+aLine);//Adding filename column to identify data 
								outBuffer.newLine();
							}
							in.close();
							
							System.out.println("filepath...."+file.getAbsolutePath());
							//zipArchiveFiles(file.getAbsolutePath(),file.lastModified());
							
							fis.close();
							/*FileMetaData fileMetaData = new FileMetaData();
							fileMetaData.setFileName(outfile.getName());	
							set1.add(fileMetaData);
							*/
							fw = new FileWriter(outMetafile, true);
							buffer = new BufferedWriter(fw);
							buffer.write(file.getName());
							buffer.newLine();
							buffer.close();
					}
					outBuffer.flush();
					outBuffer.close();
				
			}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
			System.out.println("Completed...."+Thread.currentThread().getName());
			return outfile.getCanonicalPath();
		
	
	}
	
	//To get the workbook names from list of files in specified location(i.e small file location)
	//Filenames should follow the naming pattern like {bookName}_XXXXX.csv
	/* (non-Javadoc)
	 * @see com.naga.utils.FileOperation#readMetaDataFile(java.lang.String)
	 */
	@Override
	public Set<FileMetaData> readMetaDataFile(String outPath) {

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
