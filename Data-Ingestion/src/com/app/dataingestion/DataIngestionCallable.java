package com.app.dataingestion;
/**
 * @author naga
 *
 */
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.app.dataingestion.model.FileMetaData;
import com.app.dataingestion.service.FileTypeFactory;

public class DataIngestionCallable implements Callable<String> {

	final static Logger logger = Logger.getLogger(DataIngestionCallable.class);
	String fileName=null;
	String outPath=null;
	String inPath=null;
	Set<FileMetaData> set=null;
	private CyclicBarrier barrier;
	private static ReentrantLock l;
	private String fileType;
	private static String mergeFileName;
	/**
	 * @param inPath - input source path
	 * @param fileName - bookName to identify
	 * @param outPath - output folder path
	 * @param barrier - CyclicBarrier
	 * @param l - lock 
	 * @param fileType - type of file (CSV or JSON etc)and should be case-sensitive
	 */
	public DataIngestionCallable(String inPath, String fileName,String outPath,CyclicBarrier barrier,ReentrantLock l, String fileType) {
		this.inPath = inPath;
		this.fileName = fileName;
		this.outPath = outPath;
		this.barrier = barrier;
		DataIngestionCallable.l=l;
		this.fileType = fileType;
		
	}
	
	@Override
	public String call() throws Exception {
		try {
			//l.lock();// get lock to perform safe operations
			// FileType is CSV or JSON etc.argument should be case-sensitive.
			mergeFileName = FileTypeFactory.valueOf(fileType).getFileOperation().mergeFiles(inPath, fileName, outPath);
			System.out.println(Thread.currentThread().getName() + " is waiting on barrier");
			barrier.await();
			System.out.println(Thread.currentThread().getName() + " has crossed the barrier");
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch (InterruptedException | BrokenBarrierException ex) {
			
			ex.printStackTrace();
        }

	return mergeFileName;
	}


}
