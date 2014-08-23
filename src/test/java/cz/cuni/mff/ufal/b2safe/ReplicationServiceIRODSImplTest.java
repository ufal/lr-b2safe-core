package cz.cuni.mff.ufal.b2safe;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import junit.framework.Assert;

import _org.irods.jargon.testutils.TestingPropertiesHelper;

public class ReplicationServiceIRODSImplTest {

	private static Properties testingProperties = new Properties();
	private static TestingPropertiesHelper testingPropertiesHelper = new TestingPropertiesHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	
		Properties tempProperties = testingPropertiesHelper.getTestProperties();
				
		testingProperties.put(ReplicationServiceIRODSImpl.CONFIGURATION.HOST.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_HOST_KEY));
		testingProperties.put(ReplicationServiceIRODSImpl.CONFIGURATION.PORT.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_PORT_KEY));
		testingProperties.put(
				ReplicationServiceIRODSImpl.CONFIGURATION.USER_NAME.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_USER_KEY));
		testingProperties.put(ReplicationServiceIRODSImpl.CONFIGURATION.PASSWORD.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_PASSWORD_KEY));
		testingProperties.put(ReplicationServiceIRODSImpl.CONFIGURATION.ZONE.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_ZONE_KEY));
		testingProperties.put(ReplicationServiceIRODSImpl.CONFIGURATION.HOME_DIRECTORY
				.name(), tempProperties
				.get(TestingPropertiesHelper.GENERATED_FILE_DIRECTORY_KEY));
		testingProperties.put(
				ReplicationServiceIRODSImpl.CONFIGURATION.DEFAULT_STORAGE.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_RESOURCE_KEY));
		testingProperties.put(
				ReplicationServiceIRODSImpl.CONFIGURATION.REPLICA_DIRECTORY.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_SCRATCH_DIR_KEY));		
	}

	@Test
	public final void testConnection() throws Exception {
		ReplicationServiceIRODSImpl repServ = new ReplicationServiceIRODSImpl();
		boolean resp = repServ.initialize(testingProperties);
		Assert.assertTrue(resp);
	}

	@Test
	public final void testList() throws Exception {
		ReplicationServiceIRODSImpl repServ = new ReplicationServiceIRODSImpl();
		boolean resp = repServ.initialize(testingProperties);
		Assert.assertTrue(resp);
		File temp = getTemporaryFile();
		Assert.assertTrue(temp.exists());
		repServ.replicate(temp.getAbsolutePath());
		List<String> filesOnReplicationServer = repServ.list();
		Assert.assertTrue(filesOnReplicationServer.contains(temp.getName()));
	}
	
	@Test
	public final void testReplicateFileWithMetadata() throws Exception {
		ReplicationServiceIRODSImpl repServ = new ReplicationServiceIRODSImpl();
		boolean resp = repServ.initialize(testingProperties);
		Assert.assertTrue(resp);
		File temp = getTemporaryFile();
		Assert.assertTrue(temp.exists());		
		Map<String, String> metadata = new HashMap<String, String>();
		metadata.put("TEMP1", "" + Math.random());
		metadata.put("TEMP2", "" + Math.random());		
		repServ.replicate(temp.getAbsolutePath(), metadata);
		List<String> fileList = repServ.search(metadata);
		Assert.assertFalse(fileList.isEmpty());
	}
	
	@Test
	public final void testDelete() throws Exception {
		ReplicationServiceIRODSImpl repServ = new ReplicationServiceIRODSImpl();
		boolean resp = repServ.initialize(testingProperties);
		Assert.assertTrue(resp);
		File temp = getTemporaryFile();
		Assert.assertTrue(temp.exists());		
		Map<String, String> metadata = new HashMap<String, String>();
		metadata.put("TEMP1", "" + Math.random());
		metadata.put("TEMP2", "" + Math.random());		
		repServ.replicate(temp.getAbsolutePath(), metadata);
		List<String> fileList = repServ.search(metadata);
		Assert.assertFalse(fileList.isEmpty());
		for(String file : fileList) {
			repServ.delete(file, true);
		}
		fileList = repServ.search(metadata);
		Assert.assertTrue(fileList.isEmpty());
	}
	
	@Test
	public final void testRetrieve() throws Exception {
		ReplicationServiceIRODSImpl repServ = new ReplicationServiceIRODSImpl();
		boolean resp = repServ.initialize(testingProperties);
		Assert.assertTrue(resp);
		File temp = getTemporaryFile();
		Assert.assertTrue(temp.exists());		
		Map<String, String> metadata = new HashMap<String, String>();
		metadata.put("TEMP1", "" + Math.random());
		metadata.put("TEMP2", "" + Math.random());		
		repServ.replicate(temp.getAbsolutePath(), metadata, true);
		List<String> fileList = repServ.search(metadata);
		Assert.assertFalse(fileList.isEmpty());
		for(String file : fileList) {
			String localFileName = System.getProperty("java.io.tmpdir") + File.pathSeparator + "retrieved.tmp";
			File f = new File(localFileName);
			if(f.exists()) {
				f.delete();
			}
			repServ.retriveFile(file, localFileName);
			FileInputStream fis = new FileInputStream(f);
			String md5 = DigestUtils.md5Hex(fis);
			fis.close();
			Map<String, String> retrievedMetadata = repServ.getMetadataOfDataObject(file);
			Assert.assertEquals(md5, retrievedMetadata.get("OTHER_original_checksum"));
		}		
	}		
	
	private File getTemporaryFile() throws IOException {
		File temp = File.createTempFile("test", Long.toString(System.nanoTime()), new File(System.getProperty("java.io.tmpdir")));		
		temp.deleteOnExit();
		BufferedWriter writer = new BufferedWriter(new FileWriter(temp));
		writer.append("testing testing testing ...");
		writer.close();
		return temp;
	}

}
