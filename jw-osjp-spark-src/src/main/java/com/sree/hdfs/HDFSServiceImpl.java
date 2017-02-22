package com.sree.hdfs;

import java.io.IOException;

import javax.annotation.PostConstruct;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class HDFSServiceImpl {
	Logger logger = LoggerFactory.getLogger(HDFSServiceImpl.class);
	@Autowired
	public HDFSConnectionFactory hdfsConnectionFactory;

	private FileSystem fs = null;

	private FsShell shell = null;

	@PostConstruct
	public void init() {

		fs = hdfsConnectionFactory.getFileSystem();
		shell = hdfsConnectionFactory.getShell();
	}

	public Path getPath(String relativePath) {
		return new Path(hdfsConnectionFactory.getFSRoot() + relativePath);
	}

	public RemoteIterator<LocatedFileStatus> listFiles(String hdfsPath, boolean recursive) {

		try {
			return fs.listFiles(getPath(hdfsPath), recursive);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage(), e);
		}

		return null;
	}

	public boolean mkdir(String path, boolean deleateOnExist, FsPermission permission) {
		FsPermission perm = permission != null ? permission : FsPermission.getDefault();
		try {
			Path hdfsPath = getPath(path);
			logger.debug("creating hdfs path :: [{}]", hdfsPath.toString());
			if (deleateOnExist && fs.exists(hdfsPath)) {
				logger.debug("Deleting hdfs path :: [{}]", hdfsPath.toString());
				fs.delete(hdfsPath, true);
			}
			return fs.mkdirs(hdfsPath, perm);
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage(), e);
		}
		return false;
	}

	public boolean mkdir(String path, boolean deleateOnExist, String permission) {
		try {
			Path hdfsPath = getPath(path);
			logger.debug("creating hdfs path :: [{}]", hdfsPath.toString());
			if (deleateOnExist && fs.exists(hdfsPath)) {
				logger.debug("Deleting hdfs path :: [{}]", hdfsPath.toString());
				fs.delete(hdfsPath, true);
			}
			boolean isCreated = fs.mkdirs(hdfsPath, FsPermission.getDefault());
			shell.run(new String[] { "-chmod", "-R", permission, path });
			return isCreated;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage(), e);
		}
		return false;
	}
	
	public boolean copyFromLocal(String localFilePath,String hdfsFilePath){
		Path localpath = new Path(localFilePath);
		Path hdfsPath = getPath(hdfsFilePath);
		try {
			fs.copyFromLocalFile(localpath, hdfsPath);
		
		if( !fs.exists(hdfsPath)){
			logger.info("creating HDFS Path ",hdfsFilePath);
			mkdir(hdfsFilePath, false, "777");
		}
		logger.info("copying file from local from [{}] to hdfs path location [{}] using [{}] ",localFilePath,hdfsFilePath,fs );
		return true;
		} catch (IOException e) {
			e.printStackTrace();
			logger.error(e.getMessage(),e);
		}
		return false;
		
	}
	
	public boolean deletePath(String hdfsPath,boolean recursive){
		
		Path path = getPath(hdfsPath);
		logger.info("deleting the path {} from hdfs location", hdfsPath);
		try {
			boolean status = fs.delete(path,recursive);
			if(!status){
				logger.error("Failed to deleate path {}",path);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}

}
