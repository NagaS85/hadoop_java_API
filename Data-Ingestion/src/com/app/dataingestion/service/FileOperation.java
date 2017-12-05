/**
 * 
 */
package com.app.dataingestion.service;

import java.io.IOException;
import java.util.Set;

import com.app.dataingestion.model.FileMetaData;

/**
 * @author naga
 *
 */
public interface FileOperation {
	
	/**
	 * @param inPath - 
	 * @param bookName
	 * @param outPath
	 * @return fileMetaData set
	 * @throws IOException
	 */
	public Set<FileMetaData> mergeFilesByCategory(String inPath,String bookName,String outPath) throws IOException;
	
	/**
	 * @param inPath - 
	 * @param bookName
	 * @param outPath
	 * @return fileMetaData set
	 * @throws IOException
	 */
	public String mergeFiles(String inPath,String bookName,String outPath) throws IOException;
	/**
	 * @param outPath
	 * @return fileMetaData set
	 */
	public Set<FileMetaData> readMetaDataFile(String outPath);
}
