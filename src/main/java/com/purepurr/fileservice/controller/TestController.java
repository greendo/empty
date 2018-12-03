package com.purepurr.fileservice.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

@RestController
@RequestMapping(value = "/test")
public class TestController {

    //TODO: EXAMPLE FROM https://creativedata.atlassian.net/wiki/spaces/SAP/pages/52199514/Java+-+Read+Write+files+with+HDFS
    //sudo docker-compose exec hadoop sh
    //sudo docker-compose exec hdfs getconf -confKey fs.default.name sh
    //docker exec -i -t 665b4a1e17b6 /bin/bash

    @RequestMapping(value = "/connect", method = RequestMethod.GET)
    public String testConnection() {
        try {
            getFs();
            return "ok";
        } catch (IOException e) {
            e.printStackTrace();
            return "failed";
        }
    }

    @RequestMapping(value = "/upload", method = RequestMethod.GET)
    public String testUpload() {
        try {
            //Create a path
            Path hdfswritepath = new Path("file.txt");

            //Init output stream
            FileSystem fs = getFs();

            FSDataOutputStream outputStream = fs.create(hdfswritepath);

            //Cassical output stream usage
            Date d = new Date();
            outputStream.writeBytes(d.toString());
            outputStream.close();

            return "ok";
        } catch (IOException e) {
            e.printStackTrace();
            return "failed";
        }
    }

    @RequestMapping(value = "/download", method = RequestMethod.GET)
    public String testDownload() {
        return null;
    }

    private FileSystem getFs() throws IOException {

        String hdfsuri = "hdfs://localhost:9999";

        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();

        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);

        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");

        //Get the filesystem - HDFS
        return FileSystem.get(URI.create(hdfsuri), conf);
    }
}
