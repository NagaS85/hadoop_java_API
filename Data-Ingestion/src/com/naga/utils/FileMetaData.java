package com.naga.utils;
/**
 * @author naga
 *
 */
import java.io.Serializable;

public class FileMetaData  implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String fileName;
	private String fileOffset;
	/**
	 * @return the fileName
	 */
	public String getFileName() {
		return fileName;
	}
	/**
	 * @param fileName the fileName to set
	 */
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	/**
	 * @return the fileOffset
	 */
	public String getFileOffset() {
		return fileOffset;
	}
	/**
	 * @param fileOffset the fileOffset to set
	 */
	public void setFileOffset(String fileOffset) {
		this.fileOffset = fileOffset;
	}

}
