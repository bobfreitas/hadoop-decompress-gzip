package com.freitas.serializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.freitas.commons.HDFSUtilities;
import com.freitas.mapreduce.AvroSerializerDriver;
import com.freitas.mapreduce.Compressor;
import com.freitas.mapreduce.Decompressor;

public class SerializerTest {
	
	private static final String CLUSTER_1 = "cluster1";
	private static final String IN_DIR = "/landingzone/";
	private static final String DECOMPRESSED_DIR = "/decompressedzone/di";
	private static final String OUT_DIR = "/serializedzone";
	private static final String SCHEMA_HOME = "avroschemas";
	private static File resourcesDirectory;
	
    private File testDataPath;
    private Configuration conf;
    private MiniDFSCluster cluster;
    private FileSystem fs;
    private DateTimeZone origDefault = DateTimeZone.getDefault();
    
    @BeforeClass 
    public static void setupForAllTests() {
    	resourcesDirectory = new File("src/test/resources");
    	// do some common setup here
     }
    
    @AfterClass 
    public static void teardownForAllTests() {
		// do some common clean up here
    }
    
    @Before
    public void setUp() throws Exception {
        testDataPath = new File(PathUtils.getTestDir(getClass()), 
                "miniclusters");
        
        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        conf = new HdfsConfiguration();
        
        File testDataCluster1 = new File(testDataPath, CLUSTER_1);
        String c1Path = testDataCluster1.getAbsolutePath();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);
        conf.set("yarn.app.mapreduce.am.log.level", "INFO");
        
        cluster = new MiniDFSCluster.Builder(conf).build();
        
        fs = FileSystem.get(conf);
        
        DateTimeZone.setDefault(DateTimeZone.UTC);
    }
    
    @After
    public void tearDown() throws Exception {
        Path dataDir = new Path(
                testDataPath.getParentFile().getParentFile().getParent());
        fs.delete(dataDir, true);
        File rootTestFile = new File(testDataPath.getParentFile().getParentFile().getParent());
        String rootTestDir = rootTestFile.getAbsolutePath();
        Path rootTestPath = new Path(rootTestDir);
        LocalFileSystem localFileSystem = FileSystem.getLocal(conf);
        localFileSystem.delete(rootTestPath, true);
        cluster.shutdown();
        
        DateTimeZone.setDefault(origDefault);
    }
    
    
    @Test
    public void testDecompress() throws Throwable {
    	String name = "service1";
    	String fileName = "valid.deflate";
    	Path hdfsHomeDir = fs.getHomeDirectory();
    	
    	// get the input file and copy to HDFS
    	String localInputPath = resourcesDirectory + Path.SEPARATOR + "data" + 
    			Path.SEPARATOR + name + Path.SEPARATOR + fileName;
    	Path inRootDir = new Path(hdfsHomeDir.toString() + IN_DIR);
    	String hdfsInput = inRootDir + Path.SEPARATOR + name + Path.SEPARATOR;
    	Path hdfsInputPath = new Path(hdfsInput + fileName);
    	fs.copyFromLocalFile(new Path(localInputPath), hdfsInputPath);
    	
    	// create dir for decompressing data
    	String decompressParentDir = hdfsHomeDir.toString() + DECOMPRESSED_DIR + Path.SEPARATOR + name;
    	fs.mkdirs(new Path(decompressParentDir));
    	String decompressDir = decompressParentDir + Path.SEPARATOR;
    	HDFSUtilities.deleteDir(decompressDir, conf);
    	
    	// now run the MR job
    	String[] args = {
    			hdfsInput,
    			decompressDir
    	};
    	Decompressor decompressor = new Decompressor();
    	int res = ToolRunner.run(new Configuration(), decompressor, args);
    	assertTrue(res == 0);
    	
    	List<String> fileList = HDFSUtilities.getDirFiles(fs, new Path(decompressDir), "-m-00000");
    	assertTrue(fileList.size() == 1);
    	String resultsFilePath = decompressDir + Path.SEPARATOR + fileList.get(0);
    	
    	String contents = HDFSUtilities.readFile(resultsFilePath, conf);
    	JSONObject obj = new JSONObject(contents);
    	assertEquals("server01", obj.getString("host"));
    }
    
    
    @Ignore
    @Test
    public void testCompress() throws Throwable {
    	/* 
    	 * Use this test case to create compressed test data
    	 * */
    	String name = "service1";
    	Path hdfsHomeDir = fs.getHomeDirectory();
    	
    	// get the input file and copy to HDFS
    	String localInputPath = resourcesDirectory + Path.SEPARATOR + "data" + 
    			Path.SEPARATOR + name + Path.SEPARATOR + "valid.json";
    	Path inRootDir = new Path(hdfsHomeDir.toString() + IN_DIR);
    	String hdfsInput = inRootDir + Path.SEPARATOR + name + Path.SEPARATOR + "valid.json";
    	Path hdfsInputPath = new Path(hdfsInput);
    	fs.copyFromLocalFile(new Path(localInputPath), hdfsInputPath);
    	
    	// create dir for decompressing data
    	String compressParentDir = hdfsHomeDir.toString() + "compressed" + Path.SEPARATOR + name;
    	fs.mkdirs(new Path(compressParentDir));
    	String decompressDir = compressParentDir + Path.SEPARATOR;
    	
    	// now run the MR job
    	String[] args = {
    			hdfsInput,
    			decompressDir
    	};
    	Compressor compressor = new Compressor();
    	int res = ToolRunner.run(new Configuration(), compressor, args);
    	assertTrue(res == 0);
    	
    	List<String> fileList = HDFSUtilities.getDirFiles(fs, new Path(decompressDir), "-m-00000");
    	assertTrue(fileList.size() == 1);
    	
    	String resultsFilePath = decompressDir + Path.SEPARATOR + fileList.get(0);
    	File localResultsFile = File.createTempFile("compress-results", ".deflate");
    	fs.copyToLocalFile(new Path(resultsFilePath), new Path(localResultsFile.toString()));
    	System.out.println("path: " + localResultsFile.getAbsolutePath());
    	// follow this path and copy the resulting file, rename it and move it to resources
    }
    

    @Test
    public void testService1ValidProcessing() throws Throwable {
    	String name = "service1";
    	String tableName = "service1";
    	String inputFileName = "valid.deflate";
    	Path hdfsHomeDir = fs.getHomeDirectory();

    	// get the input file and copy to HDFS
    	String localInputPath = resourcesDirectory + Path.SEPARATOR + "data" + 
    			Path.SEPARATOR + name + Path.SEPARATOR + inputFileName;
    	Path inRootDir = new Path(hdfsHomeDir.toString() + IN_DIR);
    	String hdfsInput = inRootDir + Path.SEPARATOR + name + Path.SEPARATOR ;
    	Path hdfsInputPath = new Path(hdfsInput + Path.SEPARATOR + inputFileName);
    	fs.copyFromLocalFile(new Path(localInputPath), hdfsInputPath);
    	
    	// copy the avro schema file to HDFS
    	String localSchemaPath = resourcesDirectory + Path.SEPARATOR + "schemas" + 
    			Path.SEPARATOR + tableName + ".avsc";
    	String hdfsSchemaPath = hdfsHomeDir.toString() + Path.SEPARATOR + 
    			SCHEMA_HOME + Path.SEPARATOR + tableName + Path.SEPARATOR + 
    			tableName + "-latest.avsc";
    	fs.copyFromLocalFile(new Path(localSchemaPath), new Path(hdfsSchemaPath));

    	// create dir for decompressing data
    	String decompressDir = hdfsHomeDir.toString() + DECOMPRESSED_DIR;
    	fs.mkdirs(new Path(decompressDir));
    	
    	// create output dir reference
    	Path outRootDir = new Path(hdfsHomeDir.toString() + OUT_DIR);
    	
    	// now run the MR job
    	String[] args = {
    			name,
    			tableName,
    			inRootDir.toString(),
    			outRootDir.toString(),
    			decompressDir, 
    			hdfsHomeDir.toString(),
    	};
    	AvroSerializerDriver avroSerializerDriver = new AvroSerializerDriver();
    	int res = ToolRunner.run(new Configuration(), avroSerializerDriver, args);
    	assertTrue(res == 0);
    	
    	// check the output, need to get it from HDFS and then read the Avro
    	List<String> fileList = HDFSUtilities.getDirFiles(fs, new Path(avroSerializerDriver.getOutputDirectoryPath()), "-m-00000");
    	assertTrue(fileList.size() == 1);
    	String resultsFilePath = avroSerializerDriver.getOutputDirectoryPath() + Path.SEPARATOR + fileList.get(0);
    	File localResultsFile = File.createTempFile("avro-test-results", ".avro");
    	fs.copyToLocalFile(new Path(resultsFilePath), new Path(localResultsFile.toString()));

    	Schema schema = new Schema.Parser().parse(new File(localSchemaPath));
		GenericRecord rec = new GenericData.Record(schema);
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(localResultsFile, datumReader);
		int count = 0;
		while (dataFileReader.hasNext()) {
			rec = dataFileReader.next();
			validateService1Data(rec);
			count++;
		}
		dataFileReader.close();
		assertTrue(count > 0);
    }
    
    
    private void validateService1Data(GenericRecord rec) {
    	assertEquals("server01", rec.get("host").toString());
    	assertEquals("26617492", rec.get("customerId").toString());
    	assertEquals("1543787665", rec.get("event_unixtime").toString());
    	assertEquals("f2d4c6ba60144b5da805a227db80a55f", rec.get("event_uuid").toString());
    	assertEquals("SessionConnect", rec.get("action").toString());
    	assertEquals("prod", rec.get("event_source").toString());
    	assertEquals("Windows 7", rec.get("os").toString());
    	assertEquals("d51a3785-c91c-4f75-a729-6f2f3269366f", rec.get("connectionId").toString());
    	assertEquals("cfb17d0b-426f-4133-9bcb-cc6c048012cb", rec.get("guid").toString());
    	assertEquals("https://api-server01", rec.get("apiServer").toString());
    	assertEquals("Chrome#50", rec.get("browsername").toString());
    	assertEquals("2_MX4yNjYxNzQ5Mn4xNjIuMTU4LjkwLjE4Nn4xNDU4NDY2MDY3MzY3fmlNNzkzajFDWEpydS81S3ZSUDFvWWMwNX5-", rec.get("sessionId").toString());
    }

}
