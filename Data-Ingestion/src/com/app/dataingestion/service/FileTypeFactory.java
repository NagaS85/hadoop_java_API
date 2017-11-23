/**
 * 
 */
package com.app.dataingestion.service;

/**
 * @author naga
 *
 */
public enum FileTypeFactory {
	
	CSV("csv"){
		@Override
		public FileOperation getFileOperation(){
			return new CSVFileOperation();
		}
		

	},
	JSON("json"){

		@Override
		public FileOperation getFileOperation() {
			// TODO Auto-generated method stub
			return null;
		}
	},
	TEXT("txt"){

		@Override
		public FileOperation getFileOperation() {
			// TODO Auto-generated method stub
			return null;
		}
	};

/**
 * 
 */
private FileTypeFactory(String fileType) {
	this.fileType=fileType;
}	
	
private	String fileType;
public abstract FileOperation getFileOperation();

/**
 * @return the fileType
 */
public String getFileType() {
	return fileType;
}



	

}
