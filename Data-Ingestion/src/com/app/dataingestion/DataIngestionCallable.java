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

public class DataIngestionCallable implements Callable<Set<FileMetaData>> {

	final static Logger logger = Logger.getLogger(DataIngestionCallable.class);
	String bookName=null;
	String outPath=null;
	String inPath=null;
	Set<FileMetaData> set=null;
	private CyclicBarrier barrier;
	private static ReentrantLock l;
	private String fileType;
	
	/**
	 * @param inPath - input source path
	 * @param bookName - bookName to identify
	 * @param outPath - output folder path
	 * @param barrier - CyclicBarrier
	 * @param l - lock 
	 * @param fileType - type of file (CSV or JSON etc)and should be case-sensitive
	 */
	public DataIngestionCallable(String inPath, String bookName,String outPath,CyclicBarrier barrier,ReentrantLock l, String fileType) {
		this.inPath = inPath;
		this.bookName = bookName;
		this.outPath = outPath;
		this.barrier = barrier;
		DataIngestionCallable.l=l;
		this.fileType = fileType;
		
	}
	
	@Override
	public Set<FileMetaData> call() throws Exception {
		try {
			l.lock();// get lock to perform safe operations
			// FileType is CSV or JSON etc.argument should be case-sensitive.
			set = FileTypeFactory.valueOf(fileType).getFileOperation().mergeFiles(inPath, bookName, outPath);
			//set = mergeFiles(inPath,bookName,outPath);
			l.unlock();// unlock thread 
			System.out.println(Thread.currentThread().getName() + " is waiting on barrier");
			barrier.await();
			System.out.println(Thread.currentThread().getName() + " has crossed the barrier");
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch (InterruptedException | BrokenBarrierException ex) {
			
			ex.printStackTrace();
        }

	return set;
	}


}
