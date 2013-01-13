/*
 * EtlConfig.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.config;

import java.io.File;
import java.io.InputStream;
import java.io.Serializable;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.toolsverse.cache.Cache;
import com.toolsverse.cache.CacheProvider;
import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.FieldsRepository;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.connection.EtlConnectionFactory;
import com.toolsverse.etl.core.engine.Scenario;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.metadata.JdbcMetadata;
import com.toolsverse.etl.metadata.Metadata;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.connection.ConnectionFactoryWrapper;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.license.ClientCertificate;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.CollectionUtils;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.KeyValue;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.Utils;
import com.toolsverse.util.XmlUtils;
import com.toolsverse.util.encryption.SymmetricEncryptor;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * Represents an ETL configuration file. The file contains aliases, scenarios to
 * execute, variables, etc. It's not require to have an actual file - everything
 * can be set programmatically.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlConfig implements CacheProvider<String, Object>, ObjectStorage,
        FieldsRepository, Serializable
{
    
    /** The DEFAULT_TITLE for the ETl framework. */
    public static final String DEFAULT_TITLE = "Etl Framework";
    
    /** The DEFAULT_APP_NAME for the ETl framework. */
    public static final String DEFAULT_APP_NAME = "etlprocess";
    
    // types of the actions
    
    /** Does nothing. */
    public static final int NOTHING = -1;
    
    /** Extract only. */
    public static final int EXTRACT = 0;
    
    /** Load only. */
    public static final int LOAD = 1;
    
    /** Extracts and loads data. */
    public static final int EXTRACT_LOAD = 2;
    
    /** The DEFAULT ACTION. */
    public static final int DEFAULT_ACTION = EXTRACT_LOAD;
    
    /** The EXTRACT code. */
    public static final String EXTRACT_STR = "EXTRACT";
    
    /** The LOAD code. */
    public static final String LOAD_STR = "LOAD";
    
    /** The EXTRACT and LOAD code. */
    public static final String EXTRACT_LOAD_STR = "EXTRACT_LOAD";
    
    /** The EXTRACT description. */
    public static final String EXTRACT_DESC_STR = "Extract";
    
    /** The LOAD description. */
    public static final String LOAD_DESC_STR = "Load";
    
    /** The EXTRACT and LOAD description. */
    public static final String EXTRACT_LOAD_DESC_STR = "Extract and Load";
    
    /** The default action description. */
    public static final String DEFAULT_ACTION_DESC = EXTRACT_LOAD_DESC_STR;
    
    /** The map of ACTIONS. */
    private static final Map<String, Integer> ACTIONS = new LinkedHashMap<String, Integer>();
    
    /** The map ACTION descriptions. */
    private static final Map<String, String> ACTIONS_DESC = new LinkedHashMap<String, String>();
    
    /** Process finished, with no errors. */
    public final static int RETURN_OK = 0;
    
    /** Process finished, there are errors. */
    public final static int RETURN_ERROR = 1;
    
    /** Process finished, config is not specified or cannot be found. */
    public final static int RETURN_NO_CONFIG = 2;
    
    /** Process finished, config cannot be initialized or was not initialized. */
    public final static int RETURN_CONFIG_NOT_INITIALIZED = 3;
    
    // system properties
    
    /** The SCENARIO_PATH property. */
    public final static String SCENARIO_PATH_PROP = "scenario.path";
    
    /** The ETL_CONFIG file name property. */
    public final static String ETL_CONFIG_PROP = "etl.config.name";
    
    // other
    /** The NONE. */
    public static String NONE = "none";
    
    /** The ETL_CODE. */
    public static final String ETL_CODE = "{etl_code}";
    
    // xml dictionary
    
    /** The CONFIG. */
    public static final String CONFIG = "config";
    
    /** The PROPERTIES. */
    public static final String PROPERTIES = "properties";
    
    /** The Constant LOG_STEP. */
    public static final String LOG_STEP = "log.step";
    
    /** The CONNECTION_FACTORY. */
    public static final String CONNECTION_FACTORY = "connection.factory";
    
    /** The CACHE. */
    public static final String CACHE = "cache";
    
    /** The CONNECTIONS. */
    public static final String CONNECTIONS = "connections";
    
    /** The Constant CONNECTION. */
    public static final String CONNECTION = "connection";
    
    /** The CONNECTION_ALIAS. */
    public static final String CONNECTION_ALIAS = "alias";
    
    /** The DRIVER. */
    public static final String DRIVER = "driver";
    
    /** The URL. */
    public static final String URL = "url";
    
    /** The USER_ID. */
    public static final String USER_ID = "userid";
    
    /** The PASSWORD. */
    public static final String PASSWORD = "password";
    
    /** The ALIAS_PARAMS. */
    public static final String ALIAS_PARAMS = "params";
    
    /** The INIT_SQL. */
    public static final String INIT_SQL = "sql";
    
    /** The CONNECOR_CLASS_NAME. */
    public static final String CONNECOR_CLASS_NAME = "connector";
    
    /** The ACTIVE_CONNECTIONS. */
    public static final String ACTIVE_CONNECTIONS = "active.connections";
    
    /** The SOURSES. */
    public static final String SOURSES = "sourses";
    
    /** The SOURSE. */
    public static final String SOURSE = "sourse";
    
    /** The ALIAS. */
    public static final String ALIAS = "alias";
    
    /** The CONN_NAME. */
    public static final String CONN_NAME = "name";
    
    /** The DESTINATIONS. */
    public static final String DESTINATIONS = "destinations";
    
    /** The DESTINATION. */
    public static final String DESTINATION = "destination";
    
    /** The EXECUTE. */
    public static final String EXECUTE = "execute";
    
    /** The SCENARIO. */
    public static final String SCENARIO = "scenario";
    
    /** The SCENARIO_NAME. */
    public static final String SCENARIO_NAME = "name";
    
    /** The SCENARIO_ACTION. */
    public static final String SCENARIO_ACTION = "action";
    
    /** The SCENARIO_DEST_CONNECTION_NAME. */
    public static final String SCENARIO_DEST_CONNECTION_NAME = "destination";
    
    /** The SCENARIO_VARS. */
    public static final String SCENARIO_VARS = "variables";
    
    /** The SCENARIO_VAR_VALUE. */
    public static final String SCENARIO_VAR_VALUE = "value";
    
    /** The SCENARIO_PARALLEL. */
    public static final String SCENARIO_PARALLEL = "parallel";
    
    // standard variables
    
    /** The DATA_FOLDER variable name. */
    public static final String DATA_FOLDER_VAR = "DATA_FOLDER";
    
    /** The DATA_FILENAME variable name. */
    public static final String DATA_FILENAME_VAR = "DATA_FILENAME";
    
    /** The PERSIST_METADATA variable name. */
    public static final String PERSIST_METADATA_VAR = "METADATA";
    
    /** The DELIMETER variable name. */
    public static final String DELIMETER_VAR = "DELIMETER";
    
    /** The CLEAR variable name. */
    public static final String CLEAR_VAR = "CLEAR";
    
    /** The TEMP_TABLE variable name. */
    public static final String TEMP_TABLE_VAR = "TEMP";
    
    /** The TABLE variable name. */
    public static final String TABLE_VAR = "TABLE";
    
    /** The CREATE_TABLE variable name. */
    public static final String CREATE_TABLE_VAR = "CREATE";
    
    /** The INSERT variable name. */
    public static final String INSERT_VAR = "INSERT";
    
    /** The USING variable name. */
    public static final String USING_VAR = "USING";
    
    /** The ONEXCEPTION variable name. */
    public static final String ONEXCEPTION_VAR = "ONEXCEPTION";
    
    /** The ERROR_MASK variable name. */
    public static final String ERROR_MASK_VAR = "INFO";
    
    /** The SUCCESS_MASK variable name. */
    public static final String SUCCESS_MASK_VAR = "SUCCESS";
    
    /** The USER variable name. */
    public static final String USER_VAR = "USER";
    
    /** The PASSWORD variable name. */
    public static final String PASSWORD_VAR = "PASSWORD";
    
    /** The DB_URL variable name. */
    public static final String DB_URL_VAR = "DBURL";
    
    /** The DB_PARAMS variable name. */
    public static final String DB_PARAMS_VAR = "DBPARAMS";
    
    /** The SHELL_EXT variable name. */
    public static final String SHELL_EXT_VAR = "EXT";
    
    /** The PARAMS variable name. */
    public static final String PARAMS_VAR = "PARAMS";
    
    /** The KEYS variable name. */
    public static final String KEYS_VAR = "KEYS";
    
    /** The IGNORE CHAR CASE variable name. */
    public static final String IGNORE_CASE_VAR = "IGNORECASE";
    
    /** The TRIM variable name. */
    public static final String TRIM_VAR = "TRIM";
    
    /** The THIS_ROW variable name. */
    public static final String THIS_ROW_VAR = "THIS";
    
    /** The FIELDS variable name. */
    public static final String FIELDS_VAR = "FIELDS";
    
    /** The EXCLUDE variable name. */
    public static final String EXCLUDE_VAR = "EXCLUDE";
    
    /** The INCLUDE variable name. */
    public static final String INCLUDE_VAR = "INCLUDE";
    
    // defaults
    
    /** The DEFAULT_LOG_STEP. */
    public static final int DEFAULT_LOG_STEP = 0;
    
    /** The DEFAULT DRIVER CLASS NAME. */
    public static final String DEFAULT_DRIVER = "com.toolsverse.etl.driver.GenericJdbcDriver";
    
    /** The DEFAULT_CONNECTOR. */
    public static final String DEFAULT_CONNECTOR = "com.toolsverse.etl.connector.xml.XmlConnector";
    
    /** The DEFAULT_CODEGEN_CLASS. */
    public static final String DEFAULT_CODEGEN_CLASS = "com.toolsverse.etl.core.codegen.DefaultCodeGen";
    
    /** The DEFAULT_CACHE_CLASS. */
    public static final String DEFAULT_CACHE_CLASS = "com.toolsverse.cache.SynchMemoryCache";
    
    /** The DEFAULT_CONNECTION_FACTORY_CLASS. */
    public static final String DEFAULT_CONNECTION_FACTORY_CLASS = "com.toolsverse.etl.core.connection.EtlConnectionFactoryImpl";
    
    /** The DEFAULT_DELIMITER. */
    public static final String DEFAULT_DELIMITER = "|";
    
    /** The DROP_SQL. */
    public static final String DROP_SQL = "drop";
    
    /** The CREATE_SQL. */
    public static final String CREATE_SQL = "create";
    
    // default connection names\
    
    /** The destination connection name. */
    public static String DEST_CONNECTION_NAME = "dest";
    
    /** The source connection name. */
    public static String SOURCE_CONNECTION_NAME = "source";
    
    /** The default scenarios path. */
    public static String DEFAULT_SCENARIO_PATH = "/scenario/";
    
    /** The default etl config file name. */
    public static String DEFAULT_ETL_CONFIG = "etl_config.xml";
    
    static
    {
        ACTIONS.put(EXTRACT_LOAD_STR, new Integer(EXTRACT_LOAD));
        ACTIONS.put(EXTRACT_STR, new Integer(EXTRACT));
        ACTIONS.put(LOAD_STR, new Integer(LOAD));
    }
    static
    {
        ACTIONS_DESC.put(EXTRACT_LOAD_STR, EXTRACT_LOAD_DESC_STR);
        ACTIONS_DESC.put(EXTRACT_STR, EXTRACT_DESC_STR);
        ACTIONS_DESC.put(LOAD_STR, LOAD_DESC_STR);
    }
    
    /** The xml config file name. */
    private String _xmlConfigFileName;
    
    /** The scenario path. */
    private String _scenarioPath;
    
    /** The log step. */
    private int _logStep;
    
    /** The aliases. */
    private Map<String, Alias> _aliases;
    
    /** The list of scenarios to execute. */
    private List<Scenario> _execute;
    
    /** The parallel scenarios. */
    private int _parallelScenarious;
    
    /** The cache. */
    private transient Cache<String, Object> _cache;
    
    /** The connection factory. */
    private transient EtlConnectionFactory _connectionFactory;
    
    /** The connections map. */
    private Map<String, Alias> _aliasesMap;
    
    /** The last executed code. */
    private transient String _lastExecutedCode;
    
    /** The error line. */
    private transient int _errorLine;
    
    /** The last executed file name. */
    private transient String _lastExecutedFileName;
    
    /** The drivers. */
    private transient Set<Driver> _drivers;
    
    /** The field definitions. */
    private final Map<String, Map<Integer, List<FieldDef>>> _fieldDefs;
    
    /** The connection factory class name. */
    private String _connectionFactoryClassName;
    
    /** The cache class name. */
    private String _cacheClassName;
    
    /** The global row limit. */
    private Integer _globalRowLimit;
    
    /** The automic long. Used to get unique ident names. */
    private AtomicLong _atomicLong;
    
    /**
     * Instantiates a new etl config.
     */
    public EtlConfig()
    {
        _globalRowLimit = null;
        
        _scenarioPath = populateScenarioPath();
        
        _xmlConfigFileName = populateXmlConfigFileName();
        
        _connectionFactoryClassName = DEFAULT_CONNECTION_FACTORY_CLASS;
        _cacheClassName = DEFAULT_CACHE_CLASS;
        _logStep = DEFAULT_LOG_STEP;
        _aliases = null;
        _execute = null;
        _parallelScenarious = 0;
        _cache = null;
        _connectionFactory = null;
        _aliasesMap = null;
        
        _lastExecutedCode = null;
        _errorLine = -1;
        _lastExecutedFileName = null;
        
        _drivers = new HashSet<Driver>();
        
        _fieldDefs = new HashMap<String, Map<Integer, List<FieldDef>>>();
        
        _atomicLong = new AtomicLong(0);
    }
    
    /**
     * Adds the alias to the map using alias name. The alias will be used to create a connection at run-time.
     * 
     * @param alias
     *            The alias
     * 
     * @see com.toolsverse.etl.common.Alias
     */
    public void addAlias(Alias alias)
    {
        if (_aliases == null)
            _aliases = new HashMap<String, Alias>();
        
        _aliases.put(alias.getName(), alias);
    }
    
    /**
     * Adds the alias to the map using given name. The alias will be used to create a connection at run-time.
     * 
     * @param name
     *            the name
     * @param alias
     *            the alias to add
     */
    public void addAliasToMap(String name, Alias alias)
    {
        if (_aliasesMap == null)
            _aliasesMap = new LinkedHashMap<String, Alias>();
        
        _aliasesMap.put(name, alias);
    }
    
    /**
     * Creates (if needed) and adds the connection using given alias.
     * 
     * @param con
     *            the connection
     * @param alias
     *            the alias
     * @param name
     *            the name
     * @param defName
     *            the default name
     * @return the connection
     * @throws Exception
     *             in case of any error
     */
    public Connection addConnection(Connection con, Alias alias, String name,
            String defName)
        throws Exception
    {
        con = getConnectionFactory().addConnection(con, alias, name, defName);
        
        Map<Integer, List<FieldDef>> fields = _fieldDefs.get(alias
                .getJdbcDriverClass());
        
        if (fields == null && con != null)
        {
            fields = new HashMap<Integer, List<FieldDef>>();
            
            Metadata metadata = new JdbcMetadata();
            metadata.init(new ConnectionFactoryWrapper(con, false), alias, null);
            
            _fieldDefs.put(alias.getJdbcDriverClass(),
                    metadata.discoverDatabaseTypes());
        }
        
        return con;
    }
    
    /**
     * Adds the scenario to execute.
     * 
     * @param scenario
     *            the scenario
     * @see com.toolsverse.etl.core.engine.Scenario
     */
    public void addScenario(Scenario scenario)
    {
        if (_execute == null)
            _execute = new ArrayList<Scenario>();
        
        _execute.add(scenario);
    }
    
    /**
     * Gets the action from the code.
     * 
     * @param action
     *            the code
     * @return the action
     */
    public int getAction(String action)
    {
        Integer value = ACTIONS.get(action);
        
        if (value != null)
            return value.intValue();
        else
            return DEFAULT_ACTION;
    }
    
    /**
     * Gets the action by description.
     * 
     * @param desc
     *            the description
     * @return the action by description
     */
    public int getActionByDesc(String desc)
    {
        String value = (String)CollectionUtils.getKeyFromValue(ACTIONS_DESC,
                desc);
        
        if (value != null)
            return getAction(value);
        else
            return DEFAULT_ACTION;
    }
    
    /**
     * Gets the actions allowed for the scenario. Actions can be separated by
     * "|". Example: EXTRACT|LOAD
     * 
     * @param actions
     *            the actions
     * @return the actions
     */
    public List<KeyValue> getActions(String actions)
    {
        List<KeyValue> list = new ArrayList<KeyValue>();
        
        Map<Integer, Integer> index = new HashMap<Integer, Integer>();
        
        if (!Utils.isNothing(actions))
        {
            String[] values = actions.split("\\|", -1);
            
            for (String action : values)
            {
                String desc = getDescByAction(action);
                
                if (Utils.isNothing(desc))
                    continue;
                
                Integer act = getActionByDesc(desc);
                
                if (act == null || index.containsKey(act))
                    continue;
                
                index.put(act, act);
                
                list.add(new KeyValue(act, desc));
            }
        }
        
        if (list.size() == 0)
        {
            for (String desc : ACTIONS_DESC.values())
            {
                Integer act = getActionByDesc(desc);
                
                if (act == null || index.containsKey(act))
                    continue;
                
                index.put(act, act);
                
                list.add(new KeyValue(act, desc));
            }
        }
        
        return list;
    }
    
    /**
     * Gets the map of the aliases. This map is used to create connections at run time.
     * 
     * @return the the map of the aliases
     */
    public Map<String, Alias> getAliasesMap()
    {
        return _aliasesMap;
    }
    
    /**
     * Gets the atomic long. Used to generate unique ident names.
     *
     * @return the atomic long
     */
    public AtomicLong getAtomicLong()
    {
        return _atomicLong;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.CacheProvider#getCache()
     */
    @SuppressWarnings("unchecked")
    public Cache<String, Object> getCache()
    {
        if (_cache != null)
            return _cache;
        
        _cache = (Cache<String, Object>)ObjectFactory.instance().get(
                !Utils.isNothing(_cacheClassName) ? _cacheClassName
                        : DEFAULT_CACHE_CLASS);
        
        return _cache;
    }
    
    /**
     * Gets the connection factory.
     * 
     * @return the connection factory
     */
    public EtlConnectionFactory getConnectionFactory()
    {
        if (_connectionFactory != null)
            return _connectionFactory;
        
        _connectionFactory = (EtlConnectionFactory)ObjectFactory
                .instance()
                .get(!Utils.isNothing(_connectionFactoryClassName) ? _connectionFactoryClassName
                        : DEFAULT_CONNECTION_FACTORY_CLASS);
        
        return _connectionFactory;
    }
    
    /**
     * Gets the count of the scenarios which will be executed in parallel.
     * 
     * @return the count of the scenarios which will be executed in parallel
     */
    public int getCountOfTheParallelScenarious()
    {
        return _parallelScenarious;
    }
    
    /**
     * Gets the action description by action.
     * 
     * @param action
     *            the action
     * @return the description
     */
    public String getDescByAction(String action)
    {
        return ACTIONS_DESC.get(action);
    }
    
    /**
     * Gets the drivers.
     * 
     * @return the drivers
     */
    public Set<Driver> getDrivers()
    {
        return _drivers;
    }
    
    /**
     * Gets the error line.
     * 
     * @return the error line
     */
    public int getErrorLine()
    {
        return _errorLine;
    }
    
    /**
     * Gets the <code>List</code> of the etl scenarios to execute. It is
     * possible to execute multiple scenarios either in the separate threads or
     * sequentially.
     * 
     * @return the <code>List</code> of the etl scenarios to execute
     */
    public List<Scenario> getExecute()
    {
        return _execute;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.common.FieldsRepository#getFieldDef(java.lang.String,
     * int)
     */
    public List<FieldDef> getFieldDef(String key, int type)
    {
        Map<Integer, List<FieldDef>> fields = _fieldDefs.get(key);
        
        if (fields == null)
            return null;
        
        return fields.get(type);
    }
    
    /**
     * Gets the global row limit.
     *
     * @return the global row limit
     */
    public Integer getGlobalRowLimit()
    {
        return _globalRowLimit;
    }
    
    /**
     * Gets the last executed code (code = sql).
     * 
     * @return the last executed code
     */
    public String getLastExecutedCode()
    {
        return _lastExecutedCode;
    }
    
    /**
     * Gets the last executed file name.
     * 
     * @return the last executed file name
     */
    public String getLastExecutedFileName()
    {
        return _lastExecutedFileName;
    }
    
    /**
     * Gets the "log step". Log step is a how many rows of the data set to skip
     * until log the event. For example if "log step" property set to 10 while
     * iterating through the rows the etl process will log every 10 rows.
     * Something like:
     * <p>
     * Extracting row 10 out of 1000
     * <p>
     * Extracting row 20 out of 1000
     * <p>
     * The default value is 0 which means each row\event will be logged.
     * 
     * @return the log step
     */
    public int getLogStep()
    {
        return _logStep;
    }
    
    /**
     * Gets the value of the log step property from the string.
     * 
     * @param value
     *            The <code>String</code> value of the property
     * @return the value of the log step property
     */
    private int getLogStep(String value)
    {
        if (Utils.isNothing(value))
            return DEFAULT_LOG_STEP;
        else
        {
            if (Utils.isUnsignedInt(value))
                return Integer.valueOf(value.trim()).intValue();
            else
                return DEFAULT_LOG_STEP;
        }
    }
    
    /**
     * Gets the new atomic long. Used to generate unique ident names.
     *
     * @return the new atomic long
     */
    public long getNewAtomicLong()
    {
        return _atomicLong.getAndIncrement();
    }
    
    /**
     * Gets the default path for the scenario files.
     * 
     * @return the scenario path
     */
    public String getScenarioPath()
    {
        return _scenarioPath;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#getString(java.lang.String)
     */
    public String getString(String key)
    {
        Cache<String, Object> cache = getCache();
        
        if (cache == null)
            return null;
        
        Object value = cache.get(key);
        
        return value != null ? value.toString() : null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#getValue(java.lang.String)
     */
    public Object getValue(String key)
    {
        Cache<String, Object> cache = getCache();
        
        if (cache == null)
            return null;
        
        return cache.get(key);
    }
    
    /**
     * Gets the xml config file name.
     * 
     * @return the xml config file name
     */
    public String getXmlConfigFileName()
    {
        return _xmlConfigFileName;
    }
    
    /**
     * Initializes EtlConfig.
     */
    public void init()
    {
        _connectionFactoryClassName = DEFAULT_CONNECTION_FACTORY_CLASS;
        _cacheClassName = DEFAULT_CACHE_CLASS;
        
        _connectionFactory = null;
        _cache = null;
    }
    
    /**
     * Initializes the EtlConfig by parsing default xml configuration file.
     * 
     * @return true, if successful
     * @throws Exception
     *             in case of any error
     */
    public boolean initConfigXml()
        throws Exception
    {
        return initConfigXml(null);
    }
    
    /**
     * Initializes the EtlConfig by parsing given xml configuration file.
     * 
     * @param configFileName name of the xml configuration file
     * 
     * @return true, if successful
     * @throws Exception
     *             in case of any error
     */
    public boolean initConfigXml(String configFileName)
        throws Exception
    {
        if (!Utils.isNothing(configFileName))
            setXmlConfigFileName(configFileName);
        
        InputStream dataStream = null;
        File file = null;
        XmlUtils xml = null;
        
        if (!Utils.isNothing(_xmlConfigFileName))
        {
            file = new File(_xmlConfigFileName);
            if (file != null && file.exists())
                xml = new XmlUtils(file);
            else
            {
                dataStream = EtlConfig.class.getClassLoader()
                        .getResourceAsStream(
                                SystemConfig.instance().getConfigFolderName()
                                        + _xmlConfigFileName);
                if (dataStream != null)
                    xml = new XmlUtils(dataStream);
            }
            
            if (xml != null)
                loadConfigXml(xml);
            else
            {
                Logger.log(Logger.WARNING, EtlConfig.class,
                        EtlResource.CONFIG_FILE_IS_MISSING_MSG.getValue());
                
                return false;
            }
        }
        else
        {
            Logger.log(Logger.WARNING, EtlConfig.class,
                    EtlResource.CONFIG_FILE_IS_MISSING_MSG.getValue());
            
            return false;
        }
        
        return true;
    }
    
    /**
     * Loads the config by parsing <code>XmlUtils></code> object.
     * 
     * @param xml
     *            The <code>XmlUtils></code> object.
     * 
     * @throws Exception
     *             in case of any error
     * 
     * 
     */
    private void loadConfigXml(XmlUtils xml)
        throws Exception
    {
        Node rootNode;
        Node connNode;
        Node propsNode;
        Node executeNode;
        Node soursesNode;
        Node scenarioNode;
        Node destsNode;
        Node destNode;
        Node cNode;
        Node variablesNode;
        Node variableNode;
        NodeList nodeList;
        NodeList varNodeList;
        Alias alias;
        
        String value;
        String aliasValue;
        
        boolean needSourceConnections = false;
        boolean needDestConnections = false;
        
        // config
        rootNode = xml.getFirstNodeNamed(CONFIG);
        if (rootNode == null)
        {
            Logger.log(Logger.WARNING, EtlConfig.class,
                    EtlResource.CONFIG_IS_MISSING_MSG.getValue());
            
            return;
        }
        
        // properties
        propsNode = xml.getFirstNodeNamed(rootNode, PROPERTIES);
        if (propsNode != null)
        {
            nodeList = propsNode.getChildNodes();
            
            if (nodeList != null)
                for (int i = 0; i < nodeList.getLength(); i++)
                {
                    Node pNode = nodeList.item(i);
                    
                    if ((pNode.getNodeType() == Node.TEXT_NODE)
                            || (pNode.getNodeType() == Node.COMMENT_NODE))
                        continue;
                    
                    String name = pNode.getNodeName();
                    
                    if (LOG_STEP.equals(name))
                        setLogStep(getLogStep(xml.getNodeValue(propsNode,
                                LOG_STEP)));
                    else if (CONNECTION_FACTORY.equals(name))
                        _connectionFactoryClassName = xml.getNodeValue(
                                propsNode, CONNECTION_FACTORY);
                    else if (CACHE.equals(name))
                        _cacheClassName = xml.getNodeValue(propsNode, CACHE);
                    else
                        setValue(name, xml.getValueOf(name));
                }
        }
        
        // connections
        connNode = xml.getFirstNodeNamed(rootNode, CONNECTIONS);
        if (connNode == null)
        {
            Logger.log(Logger.WARNING, EtlConfig.class,
                    EtlResource.CONNECTIONS_IS_MISSING_MSG.getValue());
            
            return;
        }
        
        // aliases
        nodeList = connNode.getChildNodes();
        
        if ((nodeList == null) || (nodeList.getLength() == 0))
        {
            Logger.log(Logger.WARNING, EtlConfig.class,
                    EtlResource.CONNECTIONS_ARE_MISSING_MSG.getValue());
            
            return;
        }
        
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            cNode = nodeList.item(i);
            
            if ((cNode.getNodeType() == Node.TEXT_NODE)
                    || (cNode.getNodeType() == Node.COMMENT_NODE))
                continue;
            
            alias = new Alias();
            
            // name
            value = xml.getStringAttribute(cNode, CONNECTION_ALIAS);
            if (Utils.isNothing(value))
                continue;
            alias.setName(value);
            
            // driver
            alias.setJdbcDriverClass(xml.getNodeValue(cNode, DRIVER));
            
            // url
            alias.setUrl(xml.getNodeValue(cNode, URL));
            
            // user id
            alias.setUserId(xml.getNodeValue(cNode, USER_ID));
            
            // password
            String password = xml.getNodeValue(cNode, PASSWORD);
            if (!Utils.isNothing(password))
                try
                {
                    password = SymmetricEncryptor.decryptPassword(
                            ClientCertificate.instance().getKey(), password);
                }
                catch (GeneralSecurityException ex)
                {
                    Logger.log(Logger.SEVERE, EtlLogger.class,
                            Resource.ERROR_DECRYPTING.getValue(), ex);
                }
            
            alias.setPassword(password);
            
            // params
            alias.setParams(xml.getNodeValue(cNode, ALIAS_PARAMS));
            
            // start sql
            alias.setInitSql(xml.getNodeValue(cNode, INIT_SQL));
            
            // start sql
            alias.setConnectorClassName(xml.getNodeValue(cNode,
                    CONNECOR_CLASS_NAME));
            
            addAlias(alias);
        }
        
        // execute
        executeNode = xml.getFirstNodeNamed(rootNode, EXECUTE);
        
        if (executeNode == null)
            return;
        
        nodeList = executeNode.getChildNodes();
        
        if ((nodeList == null) || (nodeList.getLength() == 0))
            return;
        
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            scenarioNode = nodeList.item(i);
            
            if ((scenarioNode.getNodeType() == Node.TEXT_NODE)
                    || (scenarioNode.getNodeType() == Node.COMMENT_NODE)
                    || !SCENARIO.equalsIgnoreCase(scenarioNode.getNodeName()))
                continue;
            
            // scenario name
            value = xml.getStringAttribute(scenarioNode, SCENARIO_NAME);
            
            if (Utils.isNothing(value))
                continue;
            
            Scenario scenario = new Scenario();
            
            scenario.setName(value);
            
            // scenario action
            scenario.setAction(getAction(xml.getStringAttribute(scenarioNode,
                    SCENARIO_ACTION).toUpperCase()));
            
            needSourceConnections = needSourceConnections
                    || EtlUtils.isExtract(scenario.getAction());
            needDestConnections = needSourceConnections
                    || EtlUtils.isLoad(scenario.getAction());
            
            // parallel scenario
            Boolean sParallel = xml.getBooleanAttribute(scenarioNode,
                    SCENARIO_PARALLEL);
            if (sParallel != null)
                scenario.setParallelScenario(sParallel.booleanValue());
            else
                scenario.setParallelScenario(false);
            
            if (scenario.isParallelScenario())
                _parallelScenarious++;
            
            addScenario(scenario);
            
            // scenario variables
            variablesNode = xml.getFirstNodeNamed(scenarioNode, SCENARIO_VARS);
            if (variablesNode != null)
            {
                varNodeList = variablesNode.getChildNodes();
                
                if ((varNodeList == null) || (varNodeList.getLength() == 0))
                    continue;
                else
                {
                    ListHashMap<String, Variable> variables = new ListHashMap<String, Variable>();
                    
                    for (int j = 0; j < varNodeList.getLength(); j++)
                    {
                        variableNode = varNodeList.item(j);
                        
                        if ((variableNode.getNodeType() == Node.TEXT_NODE)
                                || (variableNode.getNodeType() == Node.COMMENT_NODE))
                            continue;
                        
                        value = variableNode.getNodeName();
                        if (!Utils.isNothing(value))
                        {
                            Variable var = new Variable();
                            
                            var.setName(value);
                            
                            // variable value
                            value = xml.getStringAttribute(variableNode,
                                    SCENARIO_VAR_VALUE);
                            
                            if (Utils.isNothing(value))
                                value = "";
                            
                            var.setValue(value);
                            
                            variables.put(var.getName(), var);
                        }
                        
                    }
                    
                    if (variables.size() > 0)
                        scenario.setVariables(variables);
                }
            }
        }
        
        // active connections
        if (_aliases != null && _aliases.size() > 0)
        {
            connNode = xml.getFirstNodeNamed(rootNode, ACTIVE_CONNECTIONS);
            if (connNode == null)
            {
                Logger.log(Logger.WARNING, EtlConfig.class,
                        EtlResource.ACTIVE_CONNECTIONS_ARE_MISSING_MSG
                                .getValue());
                
                return;
            }
            
            // sourses
            soursesNode = xml.getFirstNodeNamed(connNode, SOURSES);
            if (soursesNode == null)
            {
                Logger.log(Logger.WARNING, EtlConfig.class,
                        EtlResource.SOURSES_ARE_MISSING_MSG.getValue());
                
                return;
            }
            
            nodeList = soursesNode.getChildNodes();
            
            if ((nodeList == null) || (nodeList.getLength() == 0))
            {
                Logger.log(Logger.WARNING, EtlConfig.class,
                        EtlResource.SOURSES_ARE_MISSING_MSG.getValue());
                
                return;
            }
            
            for (int i = 0; i < nodeList.getLength(); i++)
            {
                cNode = nodeList.item(i);
                
                if ((cNode.getNodeType() == Node.TEXT_NODE)
                        || (cNode.getNodeType() == Node.COMMENT_NODE))
                    continue;
                
                // alias
                aliasValue = xml.getStringAttribute(cNode, ALIAS);
                if (Utils.isNothing(aliasValue))
                {
                    Logger.log(Logger.WARNING, EtlConfig.class,
                            EtlResource.SOURSE_ALIAS_IS_MISSING_MSG.getValue());
                    
                    continue;
                }
                
                alias = _aliases.get(aliasValue);
                if (alias == null)
                {
                    Logger.log(Logger.WARNING, EtlConfig.class,
                            EtlResource.ALIAS_NOT_DEFINED_MSG.getValue());
                    
                    continue;
                }
                
                if (needSourceConnections)
                    addConnection(null, alias,
                            xml.getStringAttribute(cNode, CONN_NAME),
                            EtlConfig.SOURCE_CONNECTION_NAME);
            }
            
            // destinations
            destsNode = xml.getFirstNodeNamed(connNode, DESTINATIONS);
            
            // default: just one destination connection
            if (destsNode == null)
            {
                // destination
                destNode = xml.getFirstNodeNamed(connNode, DESTINATION);
                if (destNode == null)
                {
                    Logger.log(Logger.WARNING, EtlConfig.class,
                            EtlResource.DESTINATION_IS_MISSING_MSG.getValue());
                    
                    return;
                }
                
                // alias
                aliasValue = xml.getStringAttribute(destNode, ALIAS);
                if (Utils.isNothing(aliasValue))
                {
                    Logger.log(Logger.WARNING, EtlConfig.class,
                            EtlResource.DESTINATION_ALIAS_IS_MISSING_MSG
                                    .getValue());
                    
                    return;
                }
                
                alias = _aliases.get(aliasValue);
                if (alias == null)
                {
                    Logger.log(Logger.WARNING, EtlConfig.class,
                            EtlResource.ALIAS_NOT_DEFINED_MSG.getValue());
                    
                    return;
                }
                
                if (needDestConnections)
                    addConnection(null, alias, null,
                            EtlConfig.DEST_CONNECTION_NAME);
            }
            else
            {
                nodeList = destsNode.getChildNodes();
                
                if ((nodeList == null) || (nodeList.getLength() == 0))
                {
                    Logger.log(Logger.WARNING, EtlConfig.class,
                            EtlResource.DESTINATION_IS_MISSING_MSG.getValue());
                    
                    return;
                }
                
                for (int i = 0; i < nodeList.getLength(); i++)
                {
                    cNode = nodeList.item(i);
                    
                    if ((cNode.getNodeType() == Node.TEXT_NODE)
                            || (cNode.getNodeType() == Node.COMMENT_NODE))
                        continue;
                    
                    // alias
                    aliasValue = xml.getStringAttribute(cNode, ALIAS);
                    if (Utils.isNothing(aliasValue))
                    {
                        Logger.log(Logger.WARNING, this,
                                EtlResource.DESTINATION_ALIAS_IS_MISSING_MSG
                                        .getValue());
                        
                        continue;
                    }
                    
                    alias = _aliases.get(aliasValue);
                    if (alias == null)
                    {
                        Logger.log(Logger.WARNING, EtlConfig.class,
                                EtlResource.ALIAS_NOT_DEFINED_MSG.getValue());
                        
                        continue;
                    }
                    
                    if (needDestConnections)
                        addConnection(null, alias,
                                xml.getStringAttribute(cNode, CONN_NAME),
                                EtlConfig.DEST_CONNECTION_NAME);
                }
            }
        }
    }
    
    /**
     * Populates scenario path.
     * 
     * @return the string
     */
    private String populateScenarioPath()
    {
        return FileUtils.getUnixFolderName(FilenameUtils.normalize(SystemConfig
                .instance().getRootDataFolderPath()
                + SystemConfig.instance().getSystemProperty(SCENARIO_PATH_PROP,
                        DEFAULT_SCENARIO_PATH)));
    }
    
    /**
     * Populates xml config file name.
     * 
     * @return the string
     */
    private String populateXmlConfigFileName()
    {
        return FileUtils.getUnixFolderName(FilenameUtils.normalize(SystemConfig
                .instance().getConfigFolderName()))
                + SystemConfig.instance().getSystemProperty(ETL_CONFIG_PROP,
                        DEFAULT_ETL_CONFIG);
    }
    
    /**
     * Removes all scenarios.
     */
    public void removeScenarios()
    {
        if (_execute != null)
            _execute.clear();
    }
    
    /**
     * Sets the map of aliases.
     * 
     * @param value
     *            the value
     */
    public void setAliasesMap(Map<String, Alias> value)
    {
        _aliasesMap = value;
    }
    
    /**
     * Sets the error line.
     * 
     * @param value
     *            the new error line
     */
    public void setErrorLine(int value)
    {
        _errorLine = value;
    }
    
    /**
     * Sets the list of the etl scenarios to execute. Usually list is set by
     * parsing configuration xml file but it can be done manually by calling
     * this method. Also scenarios can be added to the list by calling
     * <code>addScenario(scenario)</code>.
     * 
     * <p>
     * <b>Example</b> of the list of the etl scenarios defined in the
     * configuration file:</br> <blockquote>
     * <dt>{@code <execute>}</dd> </br>
     * <dd>{@code <scenario name="sc1.xml" action="extract_load" parallel="Yes"
     * />}</dd>
     * </br>
     * <dd>{@code <scenario name="sc2.xml" action="extract_load" parallel="Yes"
     * />}</dd> </br>
     * <dt>{@code </execute>}</dt> </blockquote>
     * 
     * @param value
     *            the new list of scenarios to execute
     */
    public void setExecute(List<Scenario> value)
    {
        _execute = value;
    }
    
    /**
     * Sets the global row limit.
     *
     * @param value the new global row limit
     */
    public void setGlobalRowLimit(Integer value)
    {
        _globalRowLimit = value;
    }
    
    /**
     * Sets the last executed code.
     * 
     * @param value
     *            the new last executed code
     */
    public void setLastExecutedCode(String value)
    {
        _lastExecutedCode = value;
    }
    
    /**
     * Sets the last executed file name.
     * 
     * @param value
     *            the new last executed file name
     */
    public void setLastExecutedFileName(String value)
    {
        _lastExecutedFileName = value;
    }
    
    /**
     * Sets the log step. If property is not set the default value is 0 which
     * means each row\event will be logged.
     * 
     * <p>
     * <b>Example</b> of the log step property defined in the configuration
     * file:</br> <blockquote>
     * <dt>{@code <properties>}</dd> </br>
     * <dd>{@code <log_step>100</log_step>}</dd> </br>
     * <dt>{@code </properties>}</dt> </blockquote>
     * 
     * @param value
     *            The new log step
     */
    public void setLogStep(int value)
    {
        _logStep = value;
    }
    
    /**
     * Sets the scenario path.
     * 
     * @param value
     *            the new scenario path
     */
    public void setScenarioPath(String value)
    {
        _scenarioPath = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#setValue(java.lang.String,
     * java.lang.Object)
     */
    public void setValue(String key, Object value)
    {
        Cache<String, Object> cache = getCache();
        
        if (cache == null || key == null)
            return;
        
        cache.put(key, value);
    }
    
    /**
     * Sets the xml config file name.
     * 
     * @param value
     *            the new xml config file name
     */
    public void setXmlConfigFileName(String value)
    {
        if (!Utils.isNothing(FilenameUtils.getPath(value)))
            _xmlConfigFileName = value;
        else
            _xmlConfigFileName = FileUtils.getUnixFolderName(FilenameUtils
                    .normalize(SystemConfig.instance().getConfigFolderName()))
                    + value;
    }
    
    /**
     * Creates connections using previously set map of the aliases.
     * 
     * @throws Exception
     *             in case of any error
     */
    public void updateConnections()
        throws Exception
    {
        if (_aliasesMap == null || _aliasesMap.size() == 0)
            return;
        
        getConnectionFactory().releaseConnections();
        
        for (String name : _aliasesMap.keySet())
        {
            Alias alias = _aliasesMap.get(name);
            
            addConnection(null, alias, name, null);
        }
    }
    
}
