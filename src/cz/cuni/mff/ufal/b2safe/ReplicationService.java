package cz.cuni.mff.ufal.b2safe;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.irods.jargon.core.connection.IRODSAccount;
import org.irods.jargon.core.connection.IRODSServerProperties;
import org.irods.jargon.core.connection.IRODSSession;
import org.irods.jargon.core.connection.SettableJargonProperties;
import org.irods.jargon.core.connection.auth.AuthResponse;
import org.irods.jargon.core.exception.JargonException;
import org.irods.jargon.core.packinstr.TransferOptions;
import org.irods.jargon.core.pub.CollectionAO;
import org.irods.jargon.core.pub.DataObjectAO;
import org.irods.jargon.core.pub.DataTransferOperations;
import org.irods.jargon.core.pub.IRODSFileSystem;
import org.irods.jargon.core.pub.domain.AvuData;
import org.irods.jargon.core.pub.io.IRODSFile;
import org.irods.jargon.core.pub.io.IRODSFileFactory;
import org.irods.jargon.core.query.AVUQueryElement;
import org.irods.jargon.core.query.AVUQueryElement.AVUQueryPart;
import org.irods.jargon.core.query.AVUQueryOperatorEnum;
import org.irods.jargon.core.query.JargonQueryException;
import org.irods.jargon.core.query.MetaDataAndDomainData;
import org.irods.jargon.core.transfer.TransferControlBlock;
import org.irods.jargon.core.utils.LocalFileUtils;


public class ReplicationService {
	
	enum CONFIGURATION {
		HOST,
		PORT,
		USER_NAME,
		PASSWORD,
		HOME_DIRECTORY,
		ZONE,
		DEFAULT_STORAGE,
		REPLICA_DIRECTORY
	}
		
	static IRODSFileSystem irodsFileSystem = null;
	static IRODSAccount irodsAccount = null;
	static Properties configuration = null;
	
	public static AuthResponse initialize(Properties config) throws JargonException {

		configuration = new Properties();
		
		/* copy required configuration */
		for(CONFIGURATION c : CONFIGURATION.values()) {
			configuration.put(c.name(), config.get(c.name()));
		}
		
    	String host = configuration.getProperty(CONFIGURATION.HOST.name());
    	String port = configuration.getProperty(CONFIGURATION.PORT.name());
    	String username = configuration.getProperty(CONFIGURATION.USER_NAME.name());
    	String pass = configuration.getProperty(CONFIGURATION.PASSWORD.name());
    	String homedir = configuration.getProperty(CONFIGURATION.HOME_DIRECTORY.name());
    	if ( !homedir.endsWith("/") ) homedir += "/";
    	String zone = configuration.getProperty(CONFIGURATION.ZONE.name());
    	String default_storage = configuration.getProperty(CONFIGURATION.DEFAULT_STORAGE.name());
    	    	
    	irodsAccount = IRODSAccount.instance(host,
    							Integer.valueOf(port),
    							username,
    							pass,
    							homedir,
    							zone,
    							default_storage);
    	
    	
    	irodsFileSystem = IRODSFileSystem.instance();
    	    	
    	AuthResponse response = irodsFileSystem.getIRODSAccessObjectFactory().authenticateIRODSAccount(irodsAccount);

    	return response;    	
	}
	
	public static boolean isInitialized() {
		return irodsFileSystem != null;
	}
	
	public void replicate(String localFileName, Map<String, String> metadata) throws JargonException, IOException {
		replicate(localFileName, metadata, false);	
	}

	public void replicate(String localFileName, Map<String, String> metadata, boolean force) throws JargonException, IOException {
		String defaultRemoteLocation = configuration.getProperty(CONFIGURATION.REPLICA_DIRECTORY.name());
		replicate(localFileName, defaultRemoteLocation, metadata);	
	}
	
	public void replicate(String localFileName, String remoteDirectory, Map<String, String> metadata) throws JargonException, IOException {
		replicate(localFileName, remoteDirectory, metadata, false);
	}
	
	public void replicate(String localFileName, String remoteDirectory, Map<String, String> metadata, boolean force) throws JargonException, IOException {
		if(overrideJargonProperties!=null) {
			irodsFileSystem.getIrodsSession().setJargonProperties(overrideJargonProperties);
		}
    	
    	File localFile = new File(localFileName);    	
    	
    	IRODSFileFactory irodsFileFactory = irodsFileSystem .getIRODSFileFactory(irodsAccount);
    	
    	IRODSFile targetDirectory = irodsFileFactory.instanceIRODSFile(irodsAccount.getHomeDirectory() + remoteDirectory);
    	
    	if(!targetDirectory.exists()) {
    		targetDirectory.mkdir();
    	}
    	    	
    	String targetFile = targetDirectory.getCanonicalPath() + IRODSFile.PATH_SEPARATOR + localFile.getName();
    	
    	IRODSFile remoteFile = irodsFileFactory.instanceIRODSFile(targetFile);
    	
    	
    	DataTransferOperations dataTransferOperations = irodsFileSystem
    														.getIRODSAccessObjectFactory()
    														.getDataTransferOperations(irodsAccount);
    	
		TransferControlBlock controlBlock = irodsFileSystem
												.getIrodsSession()
												.buildDefaultTransferControlBlockBasedOnJargonProperties();
    	
    	if(force) {
    		controlBlock.getTransferOptions().setForceOption(TransferOptions.ForceOption.USE_FORCE);    		
    	}
    	
    	dataTransferOperations.putOperation(localFile, remoteFile, null, controlBlock);
    	
    	if(metadata==null) {
    		metadata = new HashMap<String, String>();
    	}
    	
    	metadata.put("OTHER_original_checksum", new String(LocalFileUtils.computeMD5FileCheckSumViaAbsolutePath(localFile.getAbsolutePath())));
    	metadata.put("OTHER_original_filesize", String.valueOf(localFile.length()));
    	
		if(localFile.isDirectory()) {
			addMetadataToCollection(remoteFile.getAbsolutePath(), metadata);
		} else {
			addMetadataToDataObject(remoteFile.getAbsolutePath(), metadata);
		}
	}
	
	public boolean delete(String path) throws JargonException {
		return delete(path, false);
	}

	public boolean delete(String path, boolean force) throws JargonException {
		IRODSFileFactory irodsFileFactory = irodsFileSystem .getIRODSFileFactory(irodsAccount);
		IRODSFile remoteFile = irodsFileFactory.instanceIRODSFile(path);
		if(force) {
			return remoteFile.deleteWithForceOption();
		} else {
			return remoteFile.delete();
		}
	}
	
	public void retriveFile(String remoteFileName, String localFileName) throws JargonException {
		if(overrideJargonProperties!=null) {
			irodsFileSystem.getIrodsSession().setJargonProperties(overrideJargonProperties);
		}		
    	File localFile = new File(localFileName);
    	
    	IRODSFileFactory irodsFileFactory = irodsFileSystem .getIRODSFileFactory(irodsAccount);
    	
    	IRODSFile remoteFile = irodsFileFactory.instanceIRODSFile(remoteFileName);

    	DataTransferOperations dataTransferOperations = irodsFileSystem
															.getIRODSAccessObjectFactory()
															.getDataTransferOperations(irodsAccount);
    	
    	dataTransferOperations.getOperation(remoteFile.getAbsolutePath(), localFile.getAbsolutePath(), "", null, null);    	
	}

    public void addMetadataToDataObject(String filePath, Map<String, String> metadata) throws JargonException {
    	DataObjectAO dataObjectAO = irodsFileSystem.getIRODSAccessObjectFactory().getDataObjectAO(irodsAccount);
    	for(Map.Entry<String, String> md : metadata.entrySet()) {
    		AvuData data = AvuData.instance(md.getKey(), md.getValue(), "");
    		dataObjectAO.addAVUMetadata(filePath, data);
    	}
    }
    
    public void modifyMetadataToDataObject(String filePath, Map<String, String> metadata) throws JargonException {
    	DataObjectAO dataObjectAO = irodsFileSystem.getIRODSAccessObjectFactory().getDataObjectAO(irodsAccount);
    	for(Map.Entry<String, String> md : metadata.entrySet()) {
    		AvuData data = AvuData.instance(md.getKey(), md.getValue(), "");
    		dataObjectAO.modifyAvuValueBasedOnGivenAttributeAndUnit(filePath, data);
    	}
    }
    
    public void addMetadataToCollection(String collectionPath, Map<String, String> metadata) throws JargonException {
    	CollectionAO collectionAO = irodsFileSystem.getIRODSAccessObjectFactory().getCollectionAO(irodsAccount);
    	for(Map.Entry<String, String> md : metadata.entrySet()) {
    		AvuData data = AvuData.instance(md.getKey(), md.getValue(), "");
    		collectionAO.addAVUMetadata(collectionPath, data);
    	}
    }
    
    public void modifyMetadataToCollection(String collectionPath, Map<String, String> metadata) throws JargonException {
    	CollectionAO collectionAO = irodsFileSystem.getIRODSAccessObjectFactory().getCollectionAO(irodsAccount);
    	for(Map.Entry<String, String> md : metadata.entrySet()) {
    		AvuData data = AvuData.instance(md.getKey(), md.getValue(), "");
    		collectionAO.modifyAvuValueBasedOnGivenAttributeAndUnit(collectionPath, data);
    	}
    }
    
    public List<String> list() throws JargonException {
    	return list(false);    	
    }
    
    public List<String> list(boolean returnAbsPath) throws JargonException {
    	String defaultRemoteLocation = configuration.getProperty(CONFIGURATION.REPLICA_DIRECTORY.name());
    	return list(defaultRemoteLocation, returnAbsPath);
    }
    
    public List<String> list(String remoteDirectory, boolean returnAbsPath) throws JargonException {
    	IRODSFileFactory irodsFileFactory = irodsFileSystem .getIRODSFileFactory(irodsAccount);    	
    	IRODSFile irodsDirectory = irodsFileFactory.instanceIRODSFile(irodsAccount.getHomeDirectory() + remoteDirectory);    	
    	String[] list = irodsDirectory.list();
    	List<String> retList = new ArrayList<String>();
    	for(String l : list) {
    		if(returnAbsPath) {
    			retList.add(irodsDirectory.getAbsolutePath() + IRODSFile.PATH_SEPARATOR + l);
    		} else {
    			retList.add(l);
    		}
    	}
    	return retList;
    }

    public List<String> search(Map<String, String> metadata) throws JargonException, JargonQueryException {
    	DataObjectAO cao = irodsFileSystem.getIRODSAccessObjectFactory().getDataObjectAO(irodsAccount);
    	List<AVUQueryElement> queryElements = new ArrayList<AVUQueryElement>();
    	for(Map.Entry<String, String> md : metadata.entrySet()) {
    		queryElements.add(AVUQueryElement.instanceForValueQuery(AVUQueryPart.ATTRIBUTE, AVUQueryOperatorEnum.EQUAL, md.getKey()));
    		queryElements.add(AVUQueryElement.instanceForValueQuery(AVUQueryPart.VALUE, AVUQueryOperatorEnum.EQUAL, md.getValue()));
    	}
    	List<MetaDataAndDomainData> result = cao.findMetadataValuesByMetadataQuery(queryElements);
    	List<String> retList = new ArrayList<String>();
    	for(MetaDataAndDomainData r : result) {
    		retList.add(r.getDomainObjectUniqueName());
    	}
    	return retList;
    }
        
    private SettableJargonProperties overrideJargonProperties = null;
    
    public SettableJargonProperties getSettableJargonProperties() {
    	IRODSSession irodsSession = irodsFileSystem.getIrodsSession();
    	overrideJargonProperties = new SettableJargonProperties(irodsSession.getJargonProperties());
		return overrideJargonProperties;
    }
    
    public static IRODSServerProperties gerIRODSServerProperties() {
    	return irodsFileSystem.getIrodsSession()
    				.getDiscoveredServerPropertiesCache()
    				.retrieveIRODSServerProperties(irodsAccount.getHost(), irodsAccount.getZone());
    }
    
    @Override
    protected void finalize() throws Throwable {
    	super.finalize();
    	irodsFileSystem.close();
    }
    
}
