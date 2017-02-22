package com.sree.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.MissingResourceException;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ResourceUtils;

public class HDFSConnectionFactory {

	private static final Logger logger = LoggerFactory.getLogger(HDFSConnectionFactory.class);

	@Autowired ApplicationContext applicationContexts;
	private Configuration conf;
	private FileSystem sharedFS;
	private FsShell shell;

	@PostConstruct
	public void init() {

		conf = new Configuration();

		try {
			if (ResourceUtils.getFile("config/core-site.xml").exists()) {
				conf.addResource(ResourceUtils.getFile("config/core-site.xml").getAbsolutePath());
			} else {
				throw new MissingResourceException("core-site.xml is required", null, null);
			}

			if (ResourceUtils.getFile("config/hdfs-site.xml").exists()) {
				conf.addResource(ResourceUtils.getFile("config/hdfs-site.xml").getAbsolutePath());
			} else {
				throw new MissingResourceException("hdfs-site.xml is required", null, null);
			}
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage(), e);
			e.printStackTrace();
		}

	}

	public FileSystem getSharedFileSystem() {
		try {
			if (sharedFS != null) {
				return sharedFS;
			} else {
				return FileSystem.newInstance(new URI(conf.get("fs.defaultFS")), conf);
			}
		} catch (IllegalArgumentException | IOException | URISyntaxException e) {
			logger.error(e.getMessage(), e);
		}
		return null;
	}

	public FileSystem getFileSystem() {
		try {
			return FileSystem.newInstance(new URI(conf.get("fs.defaultFS")), conf);
		} catch (IllegalArgumentException | IOException | URISyntaxException e) {
			logger.error(e.getMessage(), e);
		}
		return null;
	}

	public FsShell getShell() {
		try {
			return shell != null ? shell : new FsShell(conf);
		} catch (IllegalArgumentException e) {
			logger.error(e.getMessage(), e);
		}
		return null;
	}

	public String getFSRoot() {
		return conf.get("fs.defaultFS");
	}
}
