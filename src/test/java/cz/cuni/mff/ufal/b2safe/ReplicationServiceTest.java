package cz.cuni.mff.ufal.b2safe;

import java.util.List;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;
import junit.framework.Assert;

import _org.irods.jargon.core.connection.auth.AuthResponse;
import _org.irods.jargon.testutils.TestingPropertiesHelper;

public class ReplicationServiceTest {

	private static Properties testingProperties = new Properties();
	private static TestingPropertiesHelper testingPropertiesHelper = new TestingPropertiesHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestingPropertiesHelper testingPropertiesLoader = new TestingPropertiesHelper();
		Properties tempProperties = testingPropertiesLoader.getTestProperties();
				
		testingProperties.put(ReplicationService.CONFIGURATION.HOST.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_HOST_KEY));
		testingProperties.put(ReplicationService.CONFIGURATION.PORT.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_PORT_KEY));
		testingProperties.put(
				ReplicationService.CONFIGURATION.USER_NAME.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_USER_KEY));
		testingProperties.put(ReplicationService.CONFIGURATION.PASSWORD.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_PASSWORD_KEY));
		testingProperties.put(ReplicationService.CONFIGURATION.ZONE.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_ZONE_KEY));
		testingProperties.put(ReplicationService.CONFIGURATION.HOME_DIRECTORY
				.name(), tempProperties
				.get(TestingPropertiesHelper.GENERATED_FILE_DIRECTORY_KEY));
		testingProperties.put(
				ReplicationService.CONFIGURATION.DEFAULT_STORAGE.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_RESOURCE_KEY));
		testingProperties.put(
				ReplicationService.CONFIGURATION.REPLICA_DIRECTORY.name(),
				tempProperties.get(TestingPropertiesHelper.IRODS_SCRATCH_DIR_KEY));		
	}

	@Test
	public final void testConnection() throws Exception {
		AuthResponse resp = ReplicationService.initialize(testingProperties);
		Assert.assertTrue(resp.isSuccessful());
	}

	@Test
	public final void testList() throws Exception {
		AuthResponse resp = ReplicationService.initialize(testingProperties);
		Assert.assertTrue(resp.isSuccessful());
		ReplicationService repServ = new ReplicationService();
		List<String> test = repServ.list();		
	}

}
