/*
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.toolsverse.cache.Cache;
import com.toolsverse.cache.CacheProvider;
import com.toolsverse.etl.common.Function;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.function.DefFunctions;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.driver.DriverDiscovery;
import com.toolsverse.etl.metadata.GenericMetadataExtractor;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.Utils;
import com.toolsverse.util.XmlUtils;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * Creates etl scenario by parsing xml file.
 * 
 * @see com.toolsverse.etl.core.engine.Scenario
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlFactory
{
    // dictionary
    
    /** The SCENARIO. */
    public static final String SCENARIO = "scenario";
    
    /** The ON_SAVE. */
    public static final String ON_SAVE = "onsave";
    
    /** The ON_PERSIST. */
    public static final String ON_PERSIST = "onpersist";
    
    /** The ON_POPULATE. */
    public static final String ON_POPULATE = "onpopulate";
    
    /** The ON_EXECUTE. */
    public static final String ON_EXECUTE = "onexecute";
    
    /** The SCOPE. */
    public static final String SCOPE = "scope";
    
    /** The IS_STREAM. */
    public static final String IS_STREAM = "stream";
    
    /** The ALLOWED_ACTIONS. */
    public static final String ALLOWED_ACTIONS = "allow";
    
    /** The ACTION. */
    public static final String ACTION = "action";
    
    /** The CLASS_NAME. */
    public static final String CLASS_NAME = "class";
    
    /** The SCENARIO_NAME. */
    public static final String SCENARIO_NAME = "name";
    
    /** The SCENARIO_DESCRIPTION. */
    public static final String SCENARIO_DESCRIPTION = "description";
    
    /** The SCENARIO_DEF_FUNCTION_CLASS_NAME. */
    public static final String SCENARIO_DEF_FUNCTION_CLASS_NAME = "function";
    
    /** The METADATA_EXTRACTOR. */
    public static final String METADATA_EXTRACTOR = "metadata";
    
    /** The METADATA_EXTRACTOR_TYPES. */
    public static final String METADATA_EXTRACTOR_TYPES = "types";
    
    /** The METADATA_EXTRACTOR_INDEX. */
    public static final String METADATA_EXTRACTOR_INDEXES = "indexes";
    
    /** The METADATA_EXTRACTOR_INDEX_SUFFIX. */
    public static final String METADATA_EXTRACTOR_INDEX_SUFFIX = "suffix";
    
    /** The SCENARIO_CODEGEN_CLASS_NAME. */
    public static final String SCENARIO_CODEGEN_CLASS_NAME = "codegen";
    
    /** The SCENARIO_CONNECTION_NAME. */
    public static final String SCENARIO_CONNECTION_NAME = "connection";
    
    /** The SCENARIO_LOOP_CODE. */
    public static final String SCENARIO_LOOP_CODE = "loop";
    
    /** The SCENARIO_LOOP_COUNT. */
    public static final String SCENARIO_LOOP_COUNT = "count";
    
    /** The SCENARIO_LOOP_VAR. */
    public static final String SCENARIO_LOOP_VAR = "variable";
    
    /** The SCENARIO_LOOP_VAR_PATTERN. */
    public static final String SCENARIO_LOOP_VAR_PATTERN = "pattern";
    
    /** The SCENARIO_LOOP_FIELD. */
    public static final String SCENARIO_LOOP_FIELD = "field";
    
    /** The SCENARIO_LOOP_LANG. */
    public static final String SCENARIO_LOOP_LANG = "looplang";
    
    /** The CONDITION_CODE. */
    public static final String CONDITION_CODE = "condition";
    
    /** The CONDITION_LANG. */
    public static final String CONDITION_LANG = "conditionlang";
    
    /** The SCENARIO_LOOP_CONNECTION. */
    public static final String SCENARIO_LOOP_CONNECTION = "loop_connection";
    
    /** The CONDITION_CONNECTION. */
    public static final String CONDITION_CONNECTION = "condition_connection";
    
    /** The CONNECTION_PARALLEL. */
    public static final String CONNECTION_PARALLEL = "parallel";
    
    /** The SCRIPT_NAME. */
    public static final String SCRIPT_NAME = "script";
    
    /** The DRIVER_CLASS_NAME. */
    public static final String DRIVER_CLASS_NAME = "driver";
    
    /** The DRIVER_NAME. */
    public static final String DRIVER_NAME = "name";
    
    /** The DRIVER_PARENT. */
    public static final String DRIVER_PARENT = "parent";
    
    /** The DRIVER_STRING_LITERAL_SIZE. */
    public static final String DRIVER_STRING_LITERAL_SIZE = "literalsize";
    
    /** The DRIVER_VARCHAR_SIZE. */
    public static final String DRIVER_VARCHAR_SIZE = "varcharsize";
    
    /** The DRIVER_CHAR_SIZE. */
    public static final String DRIVER_CHAR_SIZE = "charsize";
    
    /** The DRIVER_PRECISION. */
    public static final String DRIVER_PRECISION = "precision";
    
    /** The DRIVER_SCALE. */
    public static final String DRIVER_SCALE = "scale";
    
    /** The DRIVER_LINES_LIMIT. */
    public static final String DRIVER_LINES_LIMIT = "lineslimit";
    
    /** The DRIVER_CASE_SENSITIVE. */
    public static final String DRIVER_CASE_SENSITIVE = "case";
    
    /** The DRIVER_INIT_SQL. */
    public static final String DRIVER_INIT_SQL = "initsql";
    
    /** Auto discover driver. */
    public static final String DRIVER_AUTO = "auto";
    
    /** The SOURCE_INDEPENDENT. */
    public static final String SOURCE_INDEPENDENT = "independent";
    
    /** The SOURCE_EMPTY. */
    public static final String SOURCE_EMPTY = "empty";
    
    /** The SOURCE_MANDATORY. */
    public static final String SOURCE_MANDATORY = "mandatory";
    
    /** The SOURCES. */
    public static final String SOURCES = "sources";
    
    /** The SOURCE_NAME. */
    public static final String SOURCE_NAME = "name";
    
    /** The LINKED_SOURCE_NAME. */
    public static final String LINKED_SOURCE_NAME = "source";
    
    /** The SOURCE_ENCODE. */
    public static final String SOURCE_ENCODE = "encode";
    
    /** The SOURCE_ENABLED. */
    public static final String SOURCE_ENABLED = "enabled";
    
    /** The SOURCE_PARALLEL. */
    public static final String SOURCE_PARALLEL = "parallel";
    
    /** The SOURCE_CONNECTION_NAME. */
    public static final String SOURCE_CONNECTION_NAME = "connection";
    
    /** The SOURCE_READER. */
    public static final String SOURCE_READER = "reader";
    
    /** The SOURCE_WRITER. */
    public static final String SOURCE_WRITER = "writer";
    
    /** The EXTRACT. */
    public static final String EXTRACT = "extract";
    
    /** The SQL. */
    public static final String SQL = "sql";
    
    /** The USING. */
    public static final String USING = "using";
    
    /** The SOURCE_KEY_NAME. */
    public static final String SOURCE_KEY_NAME = "keyname";
    
    /** The SOURCE_KEY_FIELD. */
    public static final String SOURCE_KEY_FIELD = "keyfield";
    
    /** The VARIABLES. */
    public static final String VARIABLES = "variables";
    
    /** The VARIABLE. */
    public static final String VARIABLE = "variable";
    
    /** The DESTINATIONS. */
    public static final String DESTINATIONS = "destinations";
    
    /** The DESTINATION_NAME. */
    public static final String DESTINATION_NAME = "name";
    
    /** The DEST_SOURCE_NAME. */
    public static final String DEST_SOURCE_NAME = "source";
    
    /** The DEST_TYPE. */
    public static final String DEST_TYPE = "type";
    
    /** The DEST_ENABLED. */
    public static final String DEST_ENABLED = "enabled";
    
    /** The DEST_PARALLEL. */
    public static final String DEST_PARALLEL = "parallel";
    
    /** The TABLE_NAME. */
    public static final String TABLE_NAME = "tablename";
    
    /** The OBJECT_NAME. */
    public static final String OBJECT_NAME = "objectname";
    
    /** The DEST_CONDITION. */
    public static final String DEST_CONDITION = "condition";
    
    /** The LOAD_ACTION. */
    public static final String LOAD_ACTION = "action";
    
    /** The LOAD_KEY. */
    public static final String LOAD_KEY = "key";
    
    /** The DEST_THEN. */
    public static final String DEST_THEN = "then";
    
    /** The DEST_ELSE. */
    public static final String DEST_ELSE = "else";
    
    /** The DEST_AFTER. */
    public static final String DEST_AFTER = "after";
    
    /** The DEST_ENCODE. */
    public static final String DEST_ENCODE = "encode";
    
    /** The DEST_TOLERATE. */
    public static final String DEST_TOLERATE = "tolerate";
    
    /** The DEST_IS_EMPTY. */
    public static final String DEST_IS_EMPTY = "empty";
    
    /** The DEST_READER. */
    public static final String DEST_READER = "reader";
    
    /** The DEST_WRITER. */
    public static final String DEST_WRITER = "writer";
    
    /** The DEST_CACHE_CLASS_NAME. */
    public static final String DEST_CACHE_CLASS_NAME = "cache";
    
    /** The DEST_CONNECTION_NAME. */
    public static final String DEST_CONNECTION_NAME = "connection";
    
    /** The DEST_CURSOR. */
    public static final String DEST_CURSOR = "cursor";
    
    /** The DEST_CURSOR_TABLE_NAME. */
    public static final String DEST_CURSOR_TABLE_NAME = "table";
    
    /** The DEST_CURSOR_SQL. */
    public static final String DEST_CURSOR_SQL = "sql";
    
    /** The DEST_CURSOR_TABLE_TYPE. */
    public static final String DEST_CURSOR_TABLE_TYPE = "type";
    
    /** The DEST_CURSOR_TABLE_ON_FINISH. */
    public static final String DEST_CURSOR_TABLE_ON_FINISH = "onfinish";
    
    /** The NO_CONNECTION. */
    public static final String NO_CONNECTION = "noconnection";
    
    /** The ON_EXCEPTION. */
    public static final String ON_EXCEPTION = "onexception";
    
    /** The EXCEPTION_MASK. */
    public static final String EXCEPTION_MASK = "mask";
    
    /** The EXCEPTION_KEY_FIELD. */
    public static final String EXCEPTION_KEY_FIELD = "key";
    
    /** The EXCEPTION_SAVEPOINT. */
    public static final String EXCEPTION_SAVEPOINT = "savepoint";
    
    /** The LOAD. */
    public static final String LOAD = "load";
    
    /** The VAR_FUNC_NAME. */
    public static final String VAR_FUNC_NAME = "function";
    
    /** The VAR_CLASS_NAME. */
    public static final String VAR_CLASS_NAME = "class";
    
    /** The VAR_FIELD_NAME. */
    public static final String VAR_FIELD_NAME = "field";
    
    /** The VAR_PK_FIELD_NAME. */
    public static final String VAR_PK_FIELD_NAME = "pk";
    
    /** The VAR_SQL. */
    public static final String VAR_SQL = "sql";
    
    /** The VAR_CODE. */
    public static final String VAR_CODE = "code";
    
    /** The VAR_LANG. */
    public static final String VAR_LANG = "lang";
    
    /** The VAR_TYPE. */
    public static final String VAR_TYPE = "type";
    
    /** The VAR_LINKED_NAME. */
    public static final String VAR_LINKED_NAME = "linked";
    
    /** The VAR_LINKED_DEST_NAME. */
    public static final String VAR_LINKED_DEST_NAME = "destination";
    
    /** The VAR_VALUE. */
    public static final String VAR_VALUE = "value";
    
    /** The VAR_LABEL. */
    public static final String VAR_LABEL = "label";
    
    /** The VAR_INCLUDE. */
    public static final String VAR_INCLUDE = "include";
    
    /** The VAR_TOLERATE. */
    public static final String VAR_TOLERATE = "tolerate";
    
    /** The VAR_TABLE_NAME. */
    public static final String VAR_TABLE_NAME = "tablename";
    
    /** The VAR_GLOBAL. */
    public static final String VAR_GLOBAL = "global";
    
    /** The VAR_DECLARE. */
    public static final String VAR_DECLARE = "declare";
    
    /** The VAR_PARAM. */
    public static final String VAR_PARAM = "param";
    
    /** The VAR_SCOPE. */
    public static final String VAR_SCOPE = "scope";
    
    /** The TASKS. */
    public static final String TASKS = "tasks";
    
    /** The BEFORE EXTRACT TASKS. */
    public static final String BEFORE_TASKS = "beforetasks";
    
    /** The AFTER LOAD TASKS. */
    public static final String AFTER_TASKS = "aftertasks";
    
    /** The TASK_NAME. */
    public static final String TASK_NAME = "name";
    
    /** The TASK_CLASS_NAME. */
    public static final String TASK_CLASS_NAME = "class";
    
    /** The TASK_CONNECTION_NAME. */
    public static final String TASK_CONNECTION_NAME = "connection";
    
    /** The TASK_TABLE_NAME. */
    public static final String TASK_TABLE_NAME = "tablename";
    
    /** The TASK_SQL. */
    public static final String TASK_SQL = "sql";
    
    /** The TASK_CODE. */
    public static final String TASK_CODE = "code";
    
    /** The TASK_CMD. */
    public static final String TASK_CMD = "cmd";
    
    /** The TASK_USING. */
    public static final String TASK_USING = "using";
    
    /** The TASK_SCOPE. */
    public static final String TASK_SCOPE = "scope";
    
    /** The TASK_COMMIT. */
    public static final String TASK_COMMIT = "commit";
    
    /** The EXECUTE. */
    public static final String EXECUTE = "execute";
    
    /** The SCENARIO_ACTION. */
    public static final String SCENARIO_ACTION = "action";
    
    /** The INNER_SCENARIO_PARELLEL. */
    public static final String INNER_SCENARIO_PARELLEL = "parallel";
    
    // parse scope (use with bitwise operators)
    /** The PARSE_ALL. */
    public static final int PARSE_ALL = 0;
    
    /** The PARSE_STRUCTURE_ONLY. */
    public static final int PARSE_STRUCTURE_ONLY = 1;
    
    /** The PARSE_RECURSVIVELY. */
    public static final int PARSE_RECURSVIVELY = 2;
    
    /** The BEFORE_TASKS_OWNER_NAME. */
    private static final String BEFORE_TASKS_OWNER_NAME = "Tasks executed before extract";
    
    /** The AFTER_TASKS_OWNER_NAME. */
    private static final String AFTER_TASKS_OWNER_NAME = "Tasks executed after load";
    
    /** The ON_EXECUTE_COMMIT. */
    private static final String ON_EXECUTE_COMMIT = "commit";
    
    /**
     * Registers the driver.
     * 
     * @param config
     *            the etl config
     * @param className
     *            the driver class name
     * @param driver
     *            the driver
     * @throws Exception
     *             in case of any error
     */
    private void addDriver(EtlConfig config, String className, Driver driver)
        throws Exception
    {
        if (config == null || config.getDrivers() == null)
            return;
        
        if (driver != null)
        {
            config.getDrivers().add(driver);
            
            return;
        }
        
        if (Utils.isNothing(className))
            return;
        
        driver = getDriver(className, null, null);
        
        if (driver != null)
            config.getDrivers().add(driver);
        
    }
    
    /**
     * Finds linked cache provider.
     *
     * @param var the variable
     * @param destinations the destinations
     * @return the linked cache provider
     */
    private CacheProvider<String, Object> findLinkedCacheProvider(Variable var,
            ListHashMap<String, Destination> destinations)
    {
        if (destinations == null || destinations.size() == 0)
            return null;
        
        for (Destination dest : destinations.getList())
        {
            Variable destVar = dest.getVariable(var.getEvalName());
            
            if (destVar != null)
            {
                if (destVar.getLinkedCacheProvider() != null)
                    return destVar.getLinkedCacheProvider();
                else
                    return dest;
            }
        }
        
        return null;
    }
    
    /**
     * Gets the driver using given string.
     * 
     * @param value
     *            the initialization string
     * @param driverDiscovery
     *            the instance of the driver discovery interface. Can be null
     * @param jdbcClassName
     *            the jdbc class name. Can be null
     * @return the driver
     * @throws Exception
     *             in case of any error
     */
    public Driver getDriver(String value, DriverDiscovery driverDiscovery,
            String jdbcClassName)
        throws Exception
    {
        Driver driver = null;
        
        if (Utils.isNothing(value))
            return null;
        
        String[] names = value.split(":", -1);
        
        if (names.length == 1)
        {
            
            driver = (Driver)ObjectFactory.instance().get(value, null,
                    names[0], null, true, true);
            
            return driver;
        }
        
        String parent = names[1];
        String stringLiteralSize = names[2];
        String varcharSize = names[3];
        String charSize = names[4];
        String precision = names[5];
        String scale = names[6];
        String linesLimit = names[7];
        String caseSensitive = names[8];
        String initSql = names[9];
        String auto = names[10];
        
        if (DRIVER_AUTO.equalsIgnoreCase(auto) && driverDiscovery != null
                && !Utils.isNothing(jdbcClassName))
        {
            driver = driverDiscovery.getDriverByJdbcClassName(jdbcClassName);
            
            if (driver != null)
                return driver;
        }
        
        driver = (Driver)ObjectFactory.instance().get(value, null, names[0],
                null, true, true);
        
        if (!Utils.isNothing(parent))
            driver.setParentDriverName(parent);
        
        if (!Utils.isNothing(stringLiteralSize))
            driver.setMaxStringLiteralSize(Integer.parseInt(stringLiteralSize));
        
        if (!Utils.isNothing(varcharSize))
            driver.setMaxVarcharSize(Integer.parseInt(varcharSize));
        
        if (!Utils.isNothing(charSize))
            driver.setMaxCharSize(Integer.parseInt(charSize));
        
        if (!Utils.isNothing(precision))
            driver.setMaxPrecision(Integer.parseInt(precision));
        
        if (!Utils.isNothing(scale))
            driver.setMaxScale(Integer.parseInt(scale));
        
        if (!Utils.isNothing(linesLimit))
            driver.setLinesLimit(Integer.parseInt(linesLimit));
        
        if (!Utils.isNothing(caseSensitive))
        {
            if (Driver.CASE_SENSITIVE_LOWER_STR.equalsIgnoreCase(caseSensitive))
                driver.setCaseSensitive(Driver.CASE_SENSITIVE_LOWER);
            else if (Driver.CASE_SENSITIVE_UPPER_STR
                    .equalsIgnoreCase(caseSensitive))
                driver.setCaseSensitive(Driver.CASE_SENSITIVE_UPPER);
            else
                driver.setCaseSensitive(Integer.parseInt(caseSensitive));
        }
        
        if (!Utils.isNothing(initSql))
            driver.setInitSql(initSql);
        
        return driver;
    }
    
    /**
     * Gets the driver.
     * 
     * @param config
     *            the etl config
     * @param xml
     *            the xml dom
     * @param rootNode
     *            the root node
     * @param driverNodeName
     *            the driver node name
     * @return the driver
     * @throws Exception
     *             in case of any error
     */
    private String getDriverClassName(EtlConfig config, XmlUtils xml,
            Node rootNode, String driverNodeName)
        throws Exception
    {
        Node driverNode = null;
        String driver = null;
        String parent = "";
        String stringLiteralSize = "";
        String varCharSize = "";
        String charSize = "";
        String precision = "";
        String scale = "";
        String linesLimit = "";
        String caseSensitive = "";
        String initSql = "";
        String auto = "";
        
        driverNode = xml.getFirstNodeNamed(rootNode, driverNodeName);
        if (driverNode != null)
        {
            driver = xml.getStringAttribute(driverNode, DRIVER_NAME);
            
            if (!Utils.isNothing(driver))
            {
                if (DRIVER_AUTO.equalsIgnoreCase(driver.trim()))
                {
                    driver = EtlConfig.DEFAULT_DRIVER;
                    
                    auto = DRIVER_AUTO;
                }
                
                parent = xml.getStringAttribute(driverNode, DRIVER_PARENT);
                if (Utils.isNothing(parent))
                    parent = "";
                
                stringLiteralSize = xml.getStringAttribute(driverNode,
                        DRIVER_STRING_LITERAL_SIZE);
                if (Utils.isNothing(stringLiteralSize))
                    stringLiteralSize = "";
                
                varCharSize = xml.getStringAttribute(driverNode,
                        DRIVER_VARCHAR_SIZE);
                if (Utils.isNothing(varCharSize))
                    varCharSize = "";
                
                charSize = xml.getStringAttribute(driverNode, DRIVER_CHAR_SIZE);
                if (Utils.isNothing(charSize))
                    charSize = "";
                
                precision = xml
                        .getStringAttribute(driverNode, DRIVER_PRECISION);
                if (Utils.isNothing(precision))
                    precision = "";
                
                scale = xml.getStringAttribute(driverNode, DRIVER_SCALE);
                if (Utils.isNothing(scale))
                    scale = "";
                
                linesLimit = xml.getStringAttribute(driverNode,
                        DRIVER_LINES_LIMIT);
                if (Utils.isNothing(linesLimit))
                    linesLimit = "";
                
                caseSensitive = xml.getStringAttribute(driverNode,
                        DRIVER_CASE_SENSITIVE);
                if (Utils.isNothing(caseSensitive))
                    caseSensitive = "";
                
                initSql = xml.getStringAttribute(driverNode, DRIVER_INIT_SQL);
                if (Utils.isNothing(initSql))
                    initSql = "";
                
            }
            else
            {
                driver = xml.getNodeValue(rootNode, driverNodeName);
                
                if (DRIVER_AUTO.equalsIgnoreCase(Utils.makeString(driver)
                        .trim()))
                {
                    driver = EtlConfig.DEFAULT_DRIVER;
                    
                    auto = DRIVER_AUTO;
                }
            }
        }
        
        if (Utils.isNothing(driver))
            return null;
        
        return driver + ":" + parent + ":" + stringLiteralSize + ":"
                + varCharSize + ":" + charSize + ":" + precision + ":" + scale
                + ":" + linesLimit + ":" + caseSensitive + ":" + initSql + ":"
                + auto;
    }
    
    /**
     * Gets the driver class name from the given string.
     * 
     * @param value
     *            the initialization string
     * @return the driver class name
     */
    public String getDriverClassName(String value)
    {
        if (Utils.isNothing(value))
            return null;
        
        String[] names = value.split(":", -1);
        
        return names[0];
    }
    
    /**
     * Gets the scenario using PARSE_ALL scope.
     * 
     * @param config
     *            the etl config
     * @param fileName
     *            the scenario file name
     * @return the scenario
     * @throws Exception
     *             in case of any error
     */
    public Scenario getScenario(EtlConfig config, String fileName)
        throws Exception
    {
        return getScenario(config, fileName, PARSE_ALL);
    }
    
    /**
     * Gets the scenario.
     * 
     * @param config
     *            the config
     * @param fileName
     *            the scenario file name
     * @param parseScope
     *            the parse scope. Possible values (bitwise operators allowed):
     *            PARSE_ALL, PARSE_STRUCTURE_ONLY, PARSE_RECURSVIVELY
     * @return the scenario
     * @throws Exception
     *             in case of any error
     */
    public Scenario getScenario(EtlConfig config, String fileName,
            int parseScope)
        throws Exception
    {
        Scenario scenario = null;
        InputStream dataStream = null;
        File file = null;
        XmlUtils xml = null;
        
        if (!Utils.isNothing(fileName))
        {
            fileName = EtlUtils.getScenarioFileName(config.getScenarioPath(),
                    fileName);
            
            file = new File(fileName);
            if (file != null && file.exists())
                xml = new XmlUtils(file);
            else
            {
                dataStream = EtlFactory.class.getClassLoader()
                        .getResourceAsStream(fileName);
                if (dataStream != null)
                    xml = new XmlUtils(dataStream);
            }
            
            if (xml != null)
                return parseScenario(config, xml, parseScope);
            else
                Logger.log(Logger.SEVERE, EtlLogger.class,
                        EtlResource.SCENARIO_FILE_IS_MISSING_MSG.getValue()
                                + fileName);
        }
        else
            Logger.log(Logger.SEVERE, EtlLogger.class,
                    EtlResource.SCENARIO_FILE_NAME_IS_MISSING_MSG.getValue());
        
        return scenario;
    }
    
    /**
     * Parses "after" tasks.
     *
     * @param config the config
     * @param xml the xml
     * @param treesNode the root node
     * @param scenario the scenario
     * @throws Exception the exception
     */
    public void parseAfterTasks(EtlConfig config, XmlUtils xml, Node treesNode,
            Scenario scenario)
        throws Exception
    {
        Destination destination = new Destination();
        destination.setNoConnection(true);
        destination.setName(AFTER_TASKS_OWNER_NAME);
        destination.setScope(Destination.SCOPE_SINGLE);
        
        parseTasks(config, xml, treesNode, scenario, destination, AFTER_TASKS);
        
        if (destination.getTasks() != null && destination.getTasks().size() > 0)
        {
            ListHashMap<String, Destination> destinations = scenario
                    .getDestinations();
            
            if (destinations == null)
            {
                destinations = new ListHashMap<String, Destination>();
                scenario.setDestinations(destinations);
            }
            
            destinations.put(destination.getName(), destination);
        }
    }
    
    /**
     * Parses "before" tasks.
     *
     * @param config the config
     * @param xml the xml
     * @param treesNode the root node
     * @param scenario the scenario
     * @param rootNodeName the root node name
     * @throws Exception the exception
     */
    public void parseBeforeTasks(EtlConfig config, XmlUtils xml,
            Node treesNode, Scenario scenario, String rootNodeName)
        throws Exception
    {
        ListHashMap<String, Source> sources = new ListHashMap<String, Source>();
        
        Source source = new Source();
        source.setNoConnection(true);
        source.setName(BEFORE_TASKS_OWNER_NAME);
        
        parseTasks(config, xml, treesNode, scenario, source, rootNodeName);
        
        if (source.getTasks() != null && source.getTasks().size() > 0)
        {
            sources.put(source.getName(), source);
            
            scenario.setSources(sources);
        }
    }
    
    /**
     * Parses the destinations.
     * 
     * @param config
     *            the etl config
     * @param xml
     *            the xml dom
     * @param treesNode
     *            the trees node
     * @param scenario
     *            the scenario
     * @throws Exception
     *             in case of any error
     */
    @SuppressWarnings("unchecked")
    public void parseDestinations(EtlConfig config, XmlUtils xml,
            Node treesNode, Scenario scenario)
        throws Exception
    {
        NodeList nodeList;
        NodeList varNodeList;
        Node node;
        Node sourceTableNode;
        Node loadNode;
        Node variablesNode;
        Node variableNode;
        Node onExeptionNode;
        Node metadataNode;
        String value;
        ListHashMap<String, Source> sources = scenario.getSources();
        
        nodeList = treesNode.getChildNodes();
        if ((nodeList == null) || (nodeList.getLength() == 0))
        {
            Logger.log(Logger.INFO, EtlLogger.class,
                    EtlResource.DESTINATIONS_ARE_MISSING_MSG.getValue());
            
            return;
        }
        
        ListHashMap<String, Destination> destinations = new ListHashMap<String, Destination>();
        
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            node = nodeList.item(i);
            if ((node.getNodeType() == Node.TEXT_NODE)
                    || (node.getNodeType() == Node.COMMENT_NODE))
                continue;
            
            // destination name
            value = xml.getNodeValue(node, DESTINATION_NAME);
            if (Utils.isNothing(value))
            {
                Logger.log(Logger.INFO, EtlLogger.class,
                        EtlResource.DESTINATION_NAME_IS_MISSING_MSG.getValue());
                
                continue;
            }
            
            value = value.toUpperCase();
            
            // lets create destination
            Destination destination = new Destination();
            
            // set destination name
            destination.setName(value);
            
            // decode
            Boolean encode = xml.getBooleanAttribute(node, DEST_ENCODE);
            if (encode != null)
                destination.setEncoded(encode.booleanValue());
            else
                destination.setEncoded(true);
            
            // ignore empty dataset
            Boolean empty = xml.getBooleanAttribute(node, DEST_IS_EMPTY);
            if (empty == null)
                empty = xml.getBooleanAttribute(node, DEST_TOLERATE);
            if (empty != null)
                destination.setIsEmpty(empty.booleanValue());
            else
                destination.setIsEmpty(false);
            
            Boolean noConnection = xml.getBooleanAttribute(node, NO_CONNECTION);
            if (Boolean.TRUE.equals(noConnection))
                destination.setNoConnection(true);
            
            // type
            destination.setType(xml.getStringAttribute(node, DEST_TYPE));
            
            // enabled\disabled
            Boolean dEnabled = xml.getBooleanAttribute(node, DEST_ENABLED);
            if (dEnabled != null)
                destination.setEnabled(dEnabled.booleanValue());
            else
                destination.setEnabled(true);
            
            // scope
            destination.setScope(xml.getStringAttribute(node, SCOPE));
            
            // conditional execution
            destination.setConditionCode(xml.getStringAttribute(node,
                    CONDITION_CODE));
            destination.setConditionConnectionName(xml.getStringAttribute(node,
                    CONDITION_CONNECTION));
            String condLang = xml.getStringAttribute(node, CONDITION_LANG);
            if (!Utils.isNothing(condLang))
                destination.setConditionLang(condLang);
            
            // parallel
            Boolean dParallel = xml.getBooleanAttribute(node, DEST_PARALLEL);
            if (dParallel != null)
                destination.setParallel(dParallel.booleanValue());
            else
                destination.setParallel(false);
            
            if (destination.isParallel())
                scenario.setParallelDests(scenario.getParallelDests() + 1);
            
            // source name
            value = xml.getNodeValue(node, DEST_SOURCE_NAME);
            if (Utils.isNothing(value))
                value = destination.getName();
            
            if (!Utils.isNothing(value) && sources != null)
            {
                value = value.toUpperCase();
                
                Source src = sources.get(value);
                if (src != null)
                {
                    src.incUsageCounter();
                    
                    if (src.isIndependent() == null)
                        src.setIsIndependent(false);
                }
                
                destination.setSource(src);
            }
            
            // object name
            value = xml.getNodeValue(node, OBJECT_NAME);
            if (Utils.isNothing(value))
                value = xml.getNodeValue(node, TABLE_NAME);
            if (!Utils.isNothing(value))
                destination.setObjectName(value);
            else
                destination.setObjectName(destination.getName());
            
            // cursor
            sourceTableNode = xml.getFirstNodeNamed(node, DEST_CURSOR);
            if (sourceTableNode != null)
            {
                destination.setCursorTableName(xml.getStringAttribute(
                        sourceTableNode, DEST_CURSOR_TABLE_NAME));
                destination.setCursorSql(xml.getStringAttribute(
                        sourceTableNode, DEST_CURSOR_SQL));
                destination.setCursorTableType(xml.getStringAttribute(
                        sourceTableNode, DEST_CURSOR_TABLE_TYPE));
                destination.setOnFinish(xml.getStringAttribute(sourceTableNode,
                        DEST_CURSOR_TABLE_ON_FINISH));
            }
            
            // metadata extractor class name
            metadataNode = xml.getFirstNodeNamed(node, METADATA_EXTRACTOR);
            if (metadataNode != null)
            {
                destination
                        .setMetadataExtractorClass(GenericMetadataExtractor.class
                                .getName());
                
                Map<String, String> attrs = xml.getAttributes(metadataNode);
                
                if (attrs != null && attrs.size() > 0)
                {
                    Boolean types = xml.getBooleanAttribute(metadataNode,
                            METADATA_EXTRACTOR_TYPES);
                    
                    if (types != null)
                        destination.setUseMetadataDataTypes(types
                                .booleanValue());
                    
                    Boolean indexes = xml.getBooleanAttribute(metadataNode,
                            METADATA_EXTRACTOR_INDEXES);
                    
                    if (indexes != null)
                        destination.setCreateIndexes(indexes.booleanValue());
                    
                    String suffix = xml.getStringAttribute(metadataNode,
                            METADATA_EXTRACTOR_INDEX_SUFFIX);
                    
                    if (!Utils.isNothing(suffix))
                    {
                        destination.setIndexSuffix(suffix);
                    }
                    
                }
            }
            else
                destination.setMetadataExtractorClass(scenario
                        .getMetadataExtractorClass());
            
            // load
            loadNode = xml.getFirstNodeNamed(node, LOAD);
            if (loadNode != null)
            {
                // stream
                Boolean isStream = xml.getBooleanAttribute(loadNode, IS_STREAM);
                
                destination.setStream(isStream != null ? isStream : false);
                
                // key
                String key = xml.getStringAttribute(loadNode, LOAD_KEY);
                
                // action
                if (!Utils.isNothing(key))
                {
                    String loadAction = xml.getStringAttribute(loadNode,
                            LOAD_ACTION);
                    
                    if (!Utils.isNothing(loadAction))
                    {
                        destination.setLoadKey(key);
                        destination.setLoadAction(loadAction);
                    }
                }
                
                // condition
                value = xml.getNodeValue(loadNode, DEST_CONDITION);
                if (!Utils.isNothing(value))
                    destination.setCondition(value);
                
                // then
                value = xml.getNodeValue(loadNode, DEST_THEN);
                if (!Utils.isNothing(value))
                    destination.setThen(value);
                
                // else
                value = xml.getNodeValue(loadNode, DEST_ELSE);
                if (!Utils.isNothing(value))
                    destination.setElse(value);
                
                // after
                value = xml.getNodeValue(loadNode, DEST_AFTER);
                if (!Utils.isNothing(value))
                    destination.setAfter(value);
                
                // sql
                value = xml.getNodeValue(loadNode, SQL);
                if (!Utils.isNothing(value))
                    destination.setSql(value);
                
                // on exception
                onExeptionNode = xml.getFirstNodeNamed(loadNode, ON_EXCEPTION);
                if (onExeptionNode != null)
                {
                    String action = xml.getStringAttribute(onExeptionNode,
                            ACTION);
                    
                    destination.setOnExceptionAction(action);
                    
                    destination.setExceptionMask(xml.getStringAttribute(
                            onExeptionNode, EXCEPTION_MASK));
                    
                    destination.setKeyFields(xml.getStringAttribute(
                            onExeptionNode, EXCEPTION_KEY_FIELD));
                    
                    Boolean savePoint = xml.getBooleanAttribute(onExeptionNode,
                            EXCEPTION_SAVEPOINT);
                    
                    if (Boolean.TRUE.equals(savePoint))
                    {
                        destination.setSavePoint(true);
                    }
                }
                
                // data reader
                Node readerNode = xml.getFirstNodeNamed(loadNode, DEST_READER);
                if (readerNode != null)
                {
                    value = xml.getStringAttribute(readerNode, CLASS_NAME);
                    
                    if (!Utils.isNothing(value))
                    {
                        destination.setDataReaderClassName(value);
                        
                        destination.setDataReaderParams(xml
                                .getAttributes(readerNode));
                    }
                    else
                    {
                        value = xml.getNodeValue(loadNode, DEST_READER);
                        if (!Utils.isNothing(value))
                            destination.setDataReaderClassName(value);
                    }
                }
                
                // data writer
                Node writerNode = xml.getFirstNodeNamed(loadNode, DEST_WRITER);
                if (writerNode != null)
                {
                    value = xml.getStringAttribute(writerNode, CLASS_NAME);
                    
                    if (!Utils.isNothing(value))
                    {
                        destination.setDataWriterClassName(value);
                        
                        destination.setDataWriterParams(xml
                                .getAttributes(writerNode));
                    }
                    else
                    {
                        value = xml.getNodeValue(loadNode, DEST_WRITER);
                        if (!Utils.isNothing(value))
                            destination.setDataWriterClassName(value);
                    }
                }
                
                // cache
                value = xml.getNodeValue(loadNode, DEST_CACHE_CLASS_NAME);
                if (Utils.isNothing(value))
                    value = EtlConfig.DEFAULT_CACHE_CLASS;
                destination.setCache((Cache<String, Object>)ObjectFactory
                        .instance().get(value));
                
                // destination connection name
                value = xml.getNodeValue(loadNode, DEST_CONNECTION_NAME);
                if (!Utils.isNothing(value))
                    destination.setConnectionName(value);
                else if (!Utils.isNothing(scenario.getDestConnectionName()))
                    destination.setConnectionName(scenario
                            .getDestConnectionName());
                else
                    destination
                            .setConnectionName(EtlConfig.DEST_CONNECTION_NAME);
                
                // destination driver name
                value = getDriverClassName(config, xml, loadNode,
                        DRIVER_CLASS_NAME);
                if (!Utils.isNothing(value))
                    destination.setDriverClassName(value);
                else if (!Utils.isNothing(scenario.getDriverClassName()))
                    destination.setDriverClassName(scenario
                            .getDriverClassName());
                addDriver(config, destination.getDriverClassName(), null);
                
                // variables
                variablesNode = xml.getFirstNodeNamed(loadNode, VARIABLES);
                if (variablesNode != null)
                {
                    varNodeList = variablesNode.getChildNodes();
                    
                    if (varNodeList != null && varNodeList.getLength() > 0)
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
                                
                                parseVariableAttrs(xml, variableNode, var);
                                
                                String tName = xml.getStringAttribute(
                                        variableNode, VAR_TABLE_NAME);
                                if (!Utils.isNothing(tName))
                                    var.setTableName(tName);
                                else
                                    var.setTableName(destination
                                            .getObjectName());
                                
                                var.setFunction(xml.getStringAttribute(
                                        variableNode, VAR_FUNC_NAME));
                                var.setFunctionClassName(xml
                                        .getStringAttribute(variableNode,
                                                VAR_CLASS_NAME));
                                var.setFieldName(xml.getStringAttribute(
                                        variableNode, VAR_FIELD_NAME));
                                var.setValue(xml.getStringAttribute(
                                        variableNode, VAR_VALUE));
                                
                                String sql = xml.getStringAttribute(
                                        variableNode, VAR_SQL);
                                if (Utils.isNothing(sql))
                                    sql = xml.getStringAttribute(variableNode,
                                            VAR_CODE);
                                var.setCode(sql);
                                
                                String lang = xml.getStringAttribute(
                                        variableNode, VAR_LANG);
                                if (!Utils.isNothing(lang))
                                    var.setLang(lang);
                                
                                var.setType(xml.getStringAttribute(
                                        variableNode, VAR_TYPE));
                                var.setLinkedVarName(xml.getStringAttribute(
                                        variableNode, VAR_LINKED_NAME));
                                var.setLabel(xml.getStringAttribute(
                                        variableNode, VAR_LABEL));
                                var.setParam(xml.getStringAttribute(
                                        variableNode, VAR_PARAM));
                                var.parseScope(xml.getStringAttribute(
                                        variableNode, VAR_SCOPE));
                                
                                String linkedDestName = xml.getStringAttribute(
                                        variableNode, VAR_LINKED_DEST_NAME);
                                if (!Utils.isNothing(linkedDestName))
                                    var.setLinkedCacheProvider(destinations
                                            .get(linkedDestName));
                                else
                                {
                                    CacheProvider<String, Object> linked = findLinkedCacheProvider(
                                            var, destinations);
                                    
                                    if (linked != null)
                                        var.setLinkedCacheProvider(linked);
                                }
                                
                                if (Utils.isNothing(var.getFunction()))
                                {
                                    if (Utils.isNothing(var.getCode()))
                                        var.setFunction(DefFunctions.DEFAULT_FUNCTION);
                                    else
                                        var.setFunction(DefFunctions.SCRIPT_FUNCTION);
                                }
                                
                                if (Utils.isNothing(var.getFunctionClassName()))
                                    if (!Utils.isNothing(destination
                                            .getDriverClassName()))
                                    {
                                        Driver driver = getDriver(
                                                destination
                                                        .getDriverClassName(),
                                                null, null);
                                        
                                        addDriver(config, null, driver);
                                    }
                                    else
                                        var.setFunctionClassName(scenario
                                                .getDefaultFunctionClass());
                                
                                Boolean tolerate = xml.getBooleanAttribute(
                                        variableNode, VAR_TOLERATE);
                                if (tolerate == null)
                                    var.setIsTolerate(false);
                                else
                                    var.setIsTolerate(tolerate.booleanValue());
                                
                                Boolean include = xml.getBooleanAttribute(
                                        variableNode, VAR_INCLUDE);
                                if (include == null)
                                {
                                    Function function = (Function)ObjectFactory
                                            .instance()
                                            .get(Utils.isNothing(var
                                                    .getFunctionClassName()) ? scenario
                                                    .getDefaultFunctionClass()
                                                    : var.getFunctionClassName(),
                                                    true);
                                    
                                    var.setIsInclude(!Utils.belongsTo(
                                            function.getExcludeFunctions(),
                                            var.getFunction()));
                                }
                                else
                                    var.setIsInclude(include.booleanValue());
                                
                                Boolean global = xml.getBooleanAttribute(
                                        variableNode, VAR_GLOBAL);
                                if (global == null)
                                    var.setIsGlobal(false);
                                else
                                    var.setIsGlobal(global.booleanValue());
                                
                                var.setDeclare(xml.getStringAttribute(
                                        variableNode, VAR_DECLARE));
                                
                                variables.put(var.getName(), var);
                            }
                            
                        }
                        
                        if (variables.size() > 0)
                            destination.setVariables(variables);
                    }
                }
            }
            
            destination.setScenarioVariables(scenario.getVariables());
            
            // tasks
            parseTasks(config, xml, node, scenario, destination, TASKS);
            
            // add destination
            destinations.put(destination.getName(), destination);
        }
        
        if (destinations.size() > 0)
            scenario.setDestinations(destinations);
        else
            Logger.log(Logger.SEVERE, EtlLogger.class,
                    EtlResource.DESTINATIONS_ARE_MISSING_MSG.getValue());
    }
    
    /**
     * Parses the inner scenario variables.
     * 
     * @param xml
     *            the xml dom
     * @param innerScenario
     *            the inner scenario
     * @param scenarioNode
     *            the scenario node
     */
    private void parseInnerScenarioVariables(XmlUtils xml,
            Scenario innerScenario, Node scenarioNode)
    {
        Node variablesNode;
        Node variableNode;
        NodeList varNodeList;
        
        String value;
        
        variablesNode = xml.getFirstNodeNamed(scenarioNode, VARIABLES);
        if (variablesNode != null)
        {
            varNodeList = variablesNode.getChildNodes();
            
            if ((varNodeList == null) || (varNodeList.getLength() == 0))
                return;
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
                        
                        parseVariableAttrs(xml, variableNode, var);
                        
                        // variable value
                        value = xml.getStringAttribute(variableNode, VAR_VALUE);
                        if (Utils.isNothing(value))
                            value = "";
                        var.setValue(value);
                        
                        var.setCode(xml.getStringAttribute(variableNode,
                                VAR_SQL));
                        var.setType(xml.getStringAttribute(variableNode,
                                VAR_TYPE));
                        
                        Boolean global = xml.getBooleanAttribute(variableNode,
                                VAR_GLOBAL);
                        if (global == null)
                            var.setIsGlobal(true);
                        else
                            var.setIsGlobal(global.booleanValue());
                        
                        var.setDeclare(xml.getStringAttribute(variableNode,
                                VAR_DECLARE));
                        
                        variables.put(var.getName(), var);
                    }
                    
                }
                
                if (variables.size() > 0)
                    innerScenario.setVariables(variables);
            }
        }
        
    }
    
    /**
     * Parses the scenario using PARSE_ALL scope.
     * 
     * @param config
     *            the etl config
     * @param code
     *            the xml as a string
     * @return the scenario
     * @throws Exception
     *             in case of any error
     */
    public Scenario parseScenario(EtlConfig config, String code)
        throws Exception
    {
        return parseScenario(config, new XmlUtils(code), PARSE_ALL);
    }
    
    /**
     * Parses the scenario.
     * 
     * @param config
     *            the etl config
     * @param code
     *            the xml as a string
     * @param parseScope
     *            the parse scope. Possible values (bitwise operators allowed):
     *            PARSE_ALL, PARSE_STRUCTURE_ONLY, PARSE_RECURSVIVELY
     * @return the scenario
     * @throws Exception
     *             in case of any error
     */
    public Scenario parseScenario(EtlConfig config, String code, int parseScope)
        throws Exception
    {
        return parseScenario(config, new XmlUtils(code), parseScope);
    }
    
    /**
     * Parses the scenario using PARSE_ALL scope.
     * 
     * @param config
     *            the erl config
     * @param xml
     *            the xml dom
     * @return the scenario
     * @throws Exception
     *             in case of any error
     */
    public Scenario parseScenario(EtlConfig config, XmlUtils xml)
        throws Exception
    {
        return parseScenario(config, xml, PARSE_ALL);
    }
    
    /**
     * Parses the scenario.
     * 
     * @param config
     *            the etl config
     * @param xml
     *            the xml dom
     * @param parseScope
     *            the parse scope. Possible values (bitwise operators allowed):
     *            PARSE_ALL, PARSE_STRUCTURE_ONLY, PARSE_RECURSVIVELY
     * @return the scenario
     * @throws Exception
     *             in case of any error
     */
    public Scenario parseScenario(EtlConfig config, XmlUtils xml, int parseScope)
        throws Exception
    {
        Node rootNode;
        Node sourcesNode;
        Node destinationNode;
        Node onSaveNode;
        Node onPersistNode;
        Node onPopulateNode;
        Node onExecuteNode;
        Node metadataNode;
        Scenario scenario = null;
        
        String value;
        String scriptName;
        String driverClassName;
        
        Node executeNode;
        Node scenarioNode;
        NodeList nodeList;
        List<Scenario> execute = null;
        
        // root
        rootNode = xml.getFirstNodeNamed(SCENARIO);
        if (rootNode == null)
        {
            Logger.log(Logger.SEVERE, EtlLogger.class,
                    EtlResource.ROOT_IS_MISSING_MSG.getValue());
            
            return null;
        }
        
        // scenario name
        value = xml.getNodeValue(rootNode, SCENARIO_NAME);
        if (Utils.isNothing(value))
        {
            Logger.log(Logger.SEVERE, EtlLogger.class,
                    EtlResource.SCENARIO_NAME_IS_MISSING_MSG.getValue());
            
            return null;
        }
        
        // script name
        scriptName = xml.getNodeValue(rootNode, SCRIPT_NAME);
        if (Utils.isNothing(scriptName))
        {
            Logger.log(Logger.SEVERE, EtlLogger.class,
                    EtlResource.SCRIPT_NAME_IS_MISSING_MSG.getValue());
            
            return null;
        }
        
        // driver class name
        if ((parseScope & PARSE_STRUCTURE_ONLY) == PARSE_STRUCTURE_ONLY)
            driverClassName = EtlConfig.DEFAULT_DRIVER;
        else
        {
            driverClassName = getDriverClassName(config, xml, rootNode,
                    DRIVER_CLASS_NAME);
            if (Utils.isNothing(driverClassName))
                driverClassName = EtlConfig.DEFAULT_DRIVER;
        }
        addDriver(config, driverClassName, null);
        
        // lets create scenario
        scenario = new Scenario();
        
        // set scenario name
        scenario.setName(value);
        
        // set script name
        scenario.setScriptName(scriptName);
        
        // set driver name
        scenario.setDriver(driverClassName);
        
        // scenario description
        scenario.setDescription(xml
                .getNodeValue(rootNode, SCENARIO_DESCRIPTION));
        
        // default function class name
        String defFuncionClass = xml.getNodeValue(rootNode,
                SCENARIO_DEF_FUNCTION_CLASS_NAME);
        if (!Utils.isNothing(defFuncionClass))
            scenario.setDefaultFunctionClass(defFuncionClass);
        else
        {
            Driver driverObj = getDriver(driverClassName, null, null);
            if (driverObj != null)
                scenario.setDefaultFunctionClass(driverObj
                        .getDefaultFunctionClass());
        }
        
        // metadata
        metadataNode = xml.getFirstNodeNamed(rootNode, METADATA_EXTRACTOR);
        if (metadataNode != null)
        {
            scenario.setMetadataExtractorClass(GenericMetadataExtractor.class
                    .getName());
            
            Map<String, String> attrs = xml.getAttributes(metadataNode);
            
            if (attrs != null && attrs.size() > 0)
            {
                Boolean types = xml.getBooleanAttribute(metadataNode,
                        METADATA_EXTRACTOR_TYPES);
                if (types != null)
                    scenario.setUseMetadataDataTypes(types.booleanValue());
            }
        }
        
        // code class
        value = xml.getNodeValue(rootNode, SCENARIO_CODEGEN_CLASS_NAME);
        if (!Utils.isNothing(value))
            scenario.setCodeGenClass(value);
        else
            scenario.setCodeGenClass(EtlConfig.DEFAULT_CODEGEN_CLASS);
        
        // connection
        value = xml.getNodeValue(rootNode, SCENARIO_CONNECTION_NAME);
        if (!Utils.isNothing(value))
            scenario.setDestConnectionName(value);
        
        // parallel connections
        Boolean cParallel = xml.getBooleanAttribute(rootNode,
                CONNECTION_PARALLEL);
        if (cParallel != null)
            scenario.setParallelConnections(cParallel.booleanValue());
        else
            scenario.setParallelConnections(false);
        
        // on save
        onSaveNode = xml.getFirstNodeNamed(rootNode, ON_SAVE);
        if (onSaveNode != null)
        {
            String action = xml.getStringAttribute(onSaveNode, ACTION);
            
            scenario.setOnSave(action);
        }
        
        // on persist
        onPersistNode = xml.getFirstNodeNamed(rootNode, ON_PERSIST);
        if (onPersistNode != null)
        {
            String action = xml.getStringAttribute(onPersistNode, ACTION);
            
            scenario.setOnPersistDataSet(action);
        }
        
        // on populate
        onPopulateNode = xml.getFirstNodeNamed(rootNode, ON_POPULATE);
        if (onPopulateNode != null)
        {
            String action = xml.getStringAttribute(onPopulateNode, ACTION);
            
            scenario.setOnPopulateDataSet(action);
        }
        
        // on execute
        onExecuteNode = xml.getFirstNodeNamed(rootNode, ON_EXECUTE);
        if (onExecuteNode != null)
        {
            String action = xml.getStringAttribute(onExecuteNode, ACTION);
            
            if (ON_EXECUTE_COMMIT.equalsIgnoreCase(action))
                scenario.setCommitEachBlock(true);
        }
        
        // allow actions
        value = xml.getNodeValue(rootNode, ALLOWED_ACTIONS);
        if (!Utils.isNothing(value))
            scenario.setAllowedActions(value);
        
        // attributes
        scenario.setAttrs(xml.getAttributes(rootNode));
        
        // variables
        parseScenarioVariables(xml, scenario, rootNode);
        
        // execute
        executeNode = xml.getFirstNodeNamed(rootNode, EXECUTE);
        if (executeNode != null)
        {
            nodeList = executeNode.getChildNodes();
            
            if (nodeList != null && nodeList.getLength() > 0)
            {
                execute = new ArrayList<Scenario>();
                
                for (int i = 0; i < nodeList.getLength(); i++)
                {
                    scenarioNode = nodeList.item(i);
                    
                    if ((scenarioNode.getNodeType() == Node.TEXT_NODE)
                            || (scenarioNode.getNodeType() == Node.COMMENT_NODE)
                            || !SCENARIO.equalsIgnoreCase(scenarioNode
                                    .getNodeName()))
                        continue;
                    
                    // scenario name
                    value = xml.getStringAttribute(scenarioNode, SCENARIO_NAME);
                    
                    if (Utils.isNothing(value))
                        continue;
                    
                    Scenario innerScenario = null;
                    
                    if ((parseScope & PARSE_RECURSVIVELY) == PARSE_RECURSVIVELY)
                        innerScenario = getScenario(config, value, parseScope);
                    else
                    {
                        innerScenario = new Scenario();
                        innerScenario.setName(value);
                    }
                    
                    if (innerScenario == null)
                        throw new Exception(Utils.format(
                                EtlResource.ERROR_INNER_NOT_EXIST.getValue(),
                                new String[] {value}));
                    
                    innerScenario.setIsInner(true);
                    
                    // scenario action
                    value = xml.getStringAttribute(scenarioNode,
                            SCENARIO_ACTION);
                    
                    if (Utils.isNothing(value))
                        innerScenario.setAction(EtlConfig.NOTHING);
                    else
                        innerScenario.setAction(config.getAction(value
                                .toUpperCase()));
                    
                    // parallel
                    Boolean scParallel = xml.getBooleanAttribute(scenarioNode,
                            INNER_SCENARIO_PARELLEL);
                    if (scParallel != null)
                        innerScenario.setParallelInnerScenario(scParallel
                                .booleanValue());
                    else
                        innerScenario.setParallelInnerScenario(false);
                    
                    if (innerScenario.isParallelInnerScenario())
                        scenario.addParallelInnerScenario();
                    
                    innerScenario.setLoopCode(xml.getStringAttribute(
                            scenarioNode, SCENARIO_LOOP_CODE));
                    
                    innerScenario.setLoopCount(xml.getStringAttribute(
                            scenarioNode, SCENARIO_LOOP_COUNT));
                    
                    innerScenario.setLoopVarName(xml.getStringAttribute(
                            scenarioNode, SCENARIO_LOOP_VAR));
                    innerScenario.setLoopVarPattern(xml.getStringAttribute(
                            scenarioNode, SCENARIO_LOOP_VAR_PATTERN));
                    innerScenario.setLoopField(xml.getStringAttribute(
                            scenarioNode, SCENARIO_LOOP_FIELD));
                    
                    String loopLang = xml.getStringAttribute(scenarioNode,
                            SCENARIO_LOOP_LANG);
                    if (!Utils.isNothing(loopLang))
                        innerScenario.setLoopLang(loopLang);
                    
                    innerScenario.setConditionCode(xml.getStringAttribute(
                            scenarioNode, CONDITION_CODE));
                    
                    String condLang = xml.getStringAttribute(scenarioNode,
                            CONDITION_LANG);
                    if (!Utils.isNothing(condLang))
                        innerScenario.setConditionLang(condLang);
                    
                    innerScenario.setConditionConnectionName(xml
                            .getStringAttribute(scenarioNode,
                                    CONDITION_CONNECTION));
                    
                    innerScenario.setLoopConnectionName(xml.getStringAttribute(
                            scenarioNode, SCENARIO_LOOP_CONNECTION));
                    
                    execute.add(innerScenario);
                    
                    // inner scenario variables
                    parseInnerScenarioVariables(xml, innerScenario,
                            scenarioNode);
                }
            }
            
            scenario.setExecute(execute);
        }
        
        if ((parseScope & PARSE_STRUCTURE_ONLY) == PARSE_STRUCTURE_ONLY)
            return scenario;
        
        scenario.setReady(true);
        
        // parse before tasks
        String beforeNodeName = null;
        Node beforeTasksNode = xml.getFirstNodeNamed(rootNode, BEFORE_TASKS);
        if (beforeTasksNode == null)
        {
            beforeTasksNode = xml.getFirstNodeNamed(rootNode, TASKS);
            if (beforeTasksNode != null)
                beforeNodeName = TASKS;
        }
        else
            beforeNodeName = BEFORE_TASKS;
        
        if (beforeTasksNode != null)
            parseBeforeTasks(config, xml, rootNode, scenario, beforeNodeName);
        
        // parse sources
        sourcesNode = xml.getFirstNodeNamed(rootNode, SOURCES);
        if (sourcesNode != null)
            parseSources(config, xml, sourcesNode, scenario);
        
        // parse destinations
        destinationNode = xml.getFirstNodeNamed(rootNode, DESTINATIONS);
        if (destinationNode != null)
            parseDestinations(config, xml, destinationNode, scenario);
        
        // parse after tasks
        Node afterTasksNode = xml.getFirstNodeNamed(rootNode, AFTER_TASKS);
        
        if (afterTasksNode != null)
            parseAfterTasks(config, xml, rootNode, scenario);
        
        return scenario;
    }
    
    /**
     * Parses the scenario variables.
     * 
     * @param xml
     *            the xml dom
     * @param scenario
     *            the scenario
     * @param rootNode
     *            the root node
     */
    public void parseScenarioVariables(XmlUtils xml, Scenario scenario,
            Node rootNode)
    {
        Node variablesNode;
        Node variableNode;
        NodeList varNodeList;
        
        String value;
        
        // variables
        variablesNode = xml.getFirstNodeNamed(rootNode, VARIABLES);
        if (variablesNode != null)
        {
            varNodeList = variablesNode.getChildNodes();
            
            if (varNodeList != null && varNodeList.getLength() > 0)
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
                        
                        parseVariableAttrs(xml, variableNode, var);
                        
                        var.setValue(xml.getStringAttribute(variableNode,
                                VAR_VALUE));
                        var.setLabel(xml.getStringAttribute(variableNode,
                                VAR_LABEL));
                        
                        var.setCode(xml.getStringAttribute(variableNode,
                                VAR_SQL));
                        var.setType(xml.getStringAttribute(variableNode,
                                VAR_TYPE));
                        
                        Boolean global = xml.getBooleanAttribute(variableNode,
                                VAR_GLOBAL);
                        if (global == null)
                            var.setIsGlobal(true);
                        else
                            var.setIsGlobal(global.booleanValue());
                        
                        parseVariableObject(xml, variableNode, var);
                        
                        variables.put(var.getName(), var);
                    }
                    
                }
                
                if (variables.size() > 0)
                    scenario.setVariables(variables);
            }
        }
        
    }
    
    /**
     * Parses the sources.
     * 
     * @param config
     *            the config
     * @param xml
     *            the xml dom
     * @param treesNode
     *            the trees node
     * @param scenario
     *            the scenario
     * @throws Exception
     *             in case of any error
     */
    public void parseSources(EtlConfig config, XmlUtils xml, Node treesNode,
            Scenario scenario)
        throws Exception
    {
        NodeList nodeList;
        NodeList varNodeList;
        Node node;
        Node extractNode;
        Node variablesNode;
        Node variableNode;
        Node onExeptionNode;
        
        String value;
        
        nodeList = treesNode.getChildNodes();
        if ((nodeList == null) || (nodeList.getLength() == 0))
        {
            Logger.log(Logger.INFO, EtlLogger.class,
                    EtlResource.SOURCES_ARE_MISSING_MSG.getValue());
            
            return;
        }
        
        ListHashMap<String, Source> sources = scenario.getSources();
        if (sources == null)
            sources = new ListHashMap<String, Source>();
        
        ListHashMap<String, Source> mandatorySources = new ListHashMap<String, Source>();
        
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            node = nodeList.item(i);
            if ((node.getNodeType() == Node.TEXT_NODE)
                    || (node.getNodeType() == Node.COMMENT_NODE))
                continue;
            
            // source name
            value = xml.getNodeValue(node, SOURCE_NAME);
            if (Utils.isNothing(value))
            {
                Logger.log(Logger.SEVERE, EtlLogger.class,
                        EtlResource.SOURCE_NAME_IS_MISSING_MSG.getValue());
                
                continue;
            }
            
            value = value.toUpperCase();
            
            // lets create source
            Source source = new Source();
            
            // set source name
            source.setName(value);
            
            // independent flag
            Boolean sIndependent = xml.getBooleanAttribute(node,
                    SOURCE_INDEPENDENT);
            if (sIndependent != null)
                source.setIsIndependent(sIndependent.booleanValue());
            
            Boolean noConnection = xml.getBooleanAttribute(node, NO_CONNECTION);
            if (Boolean.TRUE.equals(noConnection))
                source.setNoConnection(true);
            
            // always empty regardless
            Boolean sEmpty = xml.getBooleanAttribute(node, SOURCE_EMPTY);
            if (sEmpty != null)
                source.setIsEmpty(sEmpty.booleanValue());
            else
                source.setIsEmpty(false);
            
            // mandatory regardless
            Boolean sMandatory = xml
                    .getBooleanAttribute(node, SOURCE_MANDATORY);
            if (sMandatory != null)
                source.setMandatory(sMandatory.booleanValue());
            else
                source.setMandatory(false);
            
            // encode
            Boolean encode = xml.getBooleanAttribute(node, SOURCE_ENCODE);
            if (encode != null)
                source.setEncoded(encode.booleanValue());
            else
                source.setEncoded(true);
            
            // enabled\disabled
            Boolean sEnabled = xml.getBooleanAttribute(node, SOURCE_ENABLED);
            if (sEnabled != null)
                source.setEnabled(sEnabled.booleanValue());
            else
                source.setEnabled(true);
            
            // parallel
            Boolean sParallel = xml.getBooleanAttribute(node, SOURCE_PARALLEL);
            if (sParallel != null)
                source.setParallel(sParallel.booleanValue());
            else
                source.setParallel(false);
            
            if (source.isParallel())
                scenario.setParallelSources(scenario.getParallelSources() + 1);
            
            // object name
            value = xml.getNodeValue(node, OBJECT_NAME);
            if (Utils.isNothing(value))
                value = xml.getNodeValue(node, TABLE_NAME);
            if (!Utils.isNothing(value))
                source.setObjectName(value);
            
            // conditional execution
            source.setConditionCode(xml
                    .getStringAttribute(node, CONDITION_CODE));
            source.setConditionConnectionName(xml.getStringAttribute(node,
                    CONDITION_CONNECTION));
            String condLang = xml.getStringAttribute(node, CONDITION_LANG);
            if (!Utils.isNothing(condLang))
                source.setConditionLang(condLang);
            
            // on persist
            Node onPersistNode = xml.getFirstNodeNamed(node, ON_PERSIST);
            if (onPersistNode != null)
            {
                String action = xml.getStringAttribute(onPersistNode, ACTION);
                
                source.setOnPersistDataSet(action);
            }
            
            // on populate
            Node onPopulateNode = xml.getFirstNodeNamed(node, ON_POPULATE);
            if (onPopulateNode != null)
            {
                String action = xml.getStringAttribute(onPopulateNode, ACTION);
                
                source.setOnPopulateDataSet(action);
            }
            
            // linked source name
            value = xml.getNodeValue(node, LINKED_SOURCE_NAME);
            if (!Utils.isNothing(value))
            {
                source.setLinkedSourceName(value);
            }
            
            // extract
            extractNode = xml.getFirstNodeNamed(node, EXTRACT);
            if (extractNode != null)
            {
                // key name and key field
                value = xml.getStringAttribute(extractNode, SOURCE_KEY_NAME);
                if (!Utils.isNothing(value))
                    source.setKeyName(value);
                
                value = xml.getStringAttribute(extractNode, SOURCE_KEY_FIELD);
                if (!Utils.isNothing(value))
                {
                    if (Utils.isNothing(source.getKeyName()))
                        source.setKeyName(source.getName());
                    
                    source.setKeyFields(value);
                }
                
                if (!Utils.isNothing(source.getKeyName())
                        && Utils.isNothing(source.getKeyFields()))
                {
                    Logger.log(Logger.SEVERE, EtlLogger.class,
                            EtlResource.KEY_FIELD_NODE_MISSING_MSG.getValue());
                    
                    source.setKeyFields(null);
                    source.setKeyName(null);
                }
                
                // data writer
                Node writerNode = xml.getFirstNodeNamed(extractNode,
                        SOURCE_WRITER);
                if (writerNode != null)
                {
                    value = xml.getStringAttribute(writerNode, CLASS_NAME);
                    
                    if (!Utils.isNothing(value))
                    {
                        source.setDataWriterClassName(value);
                        
                        source.setDataWriterParams(xml
                                .getAttributes(writerNode));
                    }
                    else
                    {
                        value = xml.getNodeValue(extractNode, SOURCE_WRITER);
                        if (!Utils.isNothing(value))
                            source.setDataWriterClassName(value);
                    }
                }
                
                // data reader
                Node readerNode = xml.getFirstNodeNamed(extractNode,
                        SOURCE_READER);
                if (readerNode != null)
                {
                    value = xml.getStringAttribute(readerNode, CLASS_NAME);
                    
                    if (!Utils.isNothing(value))
                    {
                        source.setDataReaderClassName(value);
                        
                        source.setDataReaderParams(xml
                                .getAttributes(readerNode));
                    }
                    else
                    {
                        value = xml.getNodeValue(extractNode, SOURCE_READER);
                        if (!Utils.isNothing(value))
                            source.setDataReaderClassName(value);
                    }
                }
                
                // source driver name
                value = getDriverClassName(config, xml, extractNode,
                        DRIVER_CLASS_NAME);
                if (!Utils.isNothing(value))
                    source.setDriverClassName(value);
                else if (!Utils.isNothing(scenario.getDriverClassName()))
                    source.setDriverClassName(scenario.getDriverClassName());
                addDriver(config, source.getDriverClassName(), null);
                
                // connection
                value = xml.getNodeValue(extractNode, SOURCE_CONNECTION_NAME);
                if (!Utils.isNothing(value))
                    source.setConnectionName(value);
                else
                    source.setConnectionName(EtlConfig.SOURCE_CONNECTION_NAME);
                
                // sql
                value = xml.getNodeValue(extractNode, SQL);
                source.setSql(value);
                
                // using
                value = xml.getNodeValue(extractNode, USING);
                if (!Utils.isNothing(value))
                    source.setUsing(value);
                
                // on exception
                onExeptionNode = xml.getFirstNodeNamed(extractNode,
                        ON_EXCEPTION);
                if (onExeptionNode != null)
                {
                    String actionStr = xml.getStringAttribute(onExeptionNode,
                            ACTION);
                    
                    source.setOnExceptionAction(actionStr);
                    
                    source.setExceptionMask(xml.getStringAttribute(
                            onExeptionNode, EXCEPTION_MASK));
                }
                
                // variables
                variablesNode = xml.getFirstNodeNamed(extractNode, VARIABLES);
                if (variablesNode != null)
                {
                    varNodeList = variablesNode.getChildNodes();
                    
                    if (varNodeList != null && varNodeList.getLength() > 0)
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
                                
                                parseVariableAttrs(xml, variableNode, var);
                                
                                var.setValue(xml.getStringAttribute(
                                        variableNode, VAR_VALUE));
                                
                                variables.put(var.getName(), var);
                            }
                            
                        }
                        
                        if (variables.size() > 0)
                            source.setVariables(variables);
                        
                    }
                }
                
                // tasks
                parseTasks(config, xml, extractNode, scenario, source, TASKS);
            }
            
            // tasks
            parseTasks(config, xml, node, scenario, source, TASKS);
            
            // add source
            sources.put(source.getName(), source);
            
            if (source.isMandatory())
                mandatorySources.put(source.getName(), source);
        }
        
        if (sources.size() > 0)
            scenario.setSources(sources);
        else
            Logger.log(Logger.SEVERE, EtlLogger.class,
                    EtlResource.SOURCES_ARE_MISSING_MSG.getValue());
        
        if (mandatorySources.size() > 0)
            scenario.setMandatorySources(mandatorySources);
    }
    
    /**
     * Parses the tasks for the etl block.
     *
     * @param config the config
     * @param xml the xml dom
     * @param rootNode the root node
     * @param scenario the scenario
     * @param block the block
     * @param rootNodeName the root node name
     * @throws Exception in case of any error
     */
    public void parseTasks(EtlConfig config, XmlUtils xml, Node rootNode,
            Scenario scenario, Block block, String rootNodeName)
        throws Exception
    {
        Node tasksNode;
        Node taskNode;
        Node taskVariablesNode;
        Node onExeptionNode;
        Node taskVariableNode;
        NodeList taskNodeList;
        NodeList taskVarNodeList;
        String value;
        
        tasksNode = xml.getFirstNodeNamed(rootNode, rootNodeName);
        if (tasksNode != null)
        {
            taskNodeList = tasksNode.getChildNodes();
            
            if (taskNodeList != null && taskNodeList.getLength() > 0)
            {
                ListHashMap<String, Task> tasks = block.getTasks();
                if (tasks == null)
                    tasks = new ListHashMap<String, Task>();
                
                ListHashMap<String, Task> postTasks = block.getTasks();
                if (postTasks == null)
                    postTasks = new ListHashMap<String, Task>();
                
                ListHashMap<String, Task> inlineTasks = block.getTasks();
                if (inlineTasks == null)
                    inlineTasks = new ListHashMap<String, Task>();
                
                ListHashMap<String, Task> preEtlTasks = block.getTasks();
                if (preEtlTasks == null)
                    preEtlTasks = new ListHashMap<String, Task>();
                
                for (int j = 0; j < taskNodeList.getLength(); j++)
                {
                    taskNode = taskNodeList.item(j);
                    
                    if ((taskNode.getNodeType() == Node.TEXT_NODE)
                            || (taskNode.getNodeType() == Node.COMMENT_NODE))
                        continue;
                    
                    Task task = new Task();
                    
                    // task name
                    value = xml.getNodeValue(taskNode, TASK_NAME);
                    if (Utils.isNothing(value))
                    {
                        Logger.log(Logger.INFO, config,
                                EtlResource.TASK_NAME_IS_MISSING_MSG.getValue());
                        
                        continue;
                    }
                    task.setName(value);
                    
                    // task class name
                    value = xml.getNodeValue(taskNode, TASK_CLASS_NAME);
                    if (Utils.isNothing(value))
                    {
                        Logger.log(Logger.INFO, EtlLogger.class,
                                EtlResource.TASK_CLASS_NAME_IS_MISSING_MSG
                                        .getValue());
                        
                        continue;
                    }
                    task.setClassName(value);
                    
                    // task connection name
                    value = xml.getNodeValue(taskNode, TASK_CONNECTION_NAME);
                    if (Utils.isNothing(value))
                        task.setConnectionName(block.getConnectionName());
                    else
                        task.setConnectionName(value);
                    
                    // task driver name
                    value = getDriverClassName(config, xml, taskNode,
                            DRIVER_CLASS_NAME);
                    task.setDriverClassName(value);
                    addDriver(config, value, null);
                    
                    // task table name
                    value = xml.getNodeValue(taskNode, TASK_TABLE_NAME);
                    if (Utils.isNothing(value))
                        task.setTableName(task.getName());
                    else
                        task.setTableName(value);
                    
                    Boolean noConnection = xml.getBooleanAttribute(taskNode,
                            NO_CONNECTION);
                    if (Boolean.TRUE.equals(noConnection))
                        task.setNoConnection(true);
                    
                    // sql/cmd/code
                    value = xml.getNodeValue(taskNode, TASK_SQL);
                    if (Utils.isNothing(value))
                        value = xml.getNodeValue(taskNode, TASK_CMD);
                    if (Utils.isNothing(value))
                        value = xml.getNodeValue(taskNode, TASK_CODE);
                    task.setCode(value);
                    
                    // using
                    task.setUsing(xml.getNodeValue(taskNode, TASK_USING));
                    
                    // commit
                    Boolean commit = xml.getBooleanAttribute(taskNode,
                            TASK_COMMIT);
                    task.setCommitWhenDone(commit == null
                            || Boolean.TRUE.equals(commit));
                    
                    // on exception
                    onExeptionNode = xml.getFirstNodeNamed(taskNode,
                            ON_EXCEPTION);
                    if (onExeptionNode != null)
                    {
                        String actionStr = xml.getStringAttribute(
                                onExeptionNode, ACTION);
                        
                        task.setOnExceptionAction(actionStr);
                        
                        task.setExceptionMask(xml.getStringAttribute(
                                onExeptionNode, EXCEPTION_MASK));
                    }
                    
                    // variables
                    taskVariablesNode = xml.getFirstNodeNamed(taskNode,
                            VARIABLES);
                    if (taskVariablesNode != null)
                    {
                        taskVarNodeList = taskVariablesNode.getChildNodes();
                        
                        if (taskVarNodeList != null
                                && taskVarNodeList.getLength() > 0)
                        {
                            ListHashMap<String, Variable> variables = new ListHashMap<String, Variable>();
                            
                            for (int ind = 0; ind < taskVarNodeList.getLength(); ind++)
                            {
                                taskVariableNode = taskVarNodeList.item(ind);
                                
                                if ((taskVariableNode.getNodeType() == Node.TEXT_NODE)
                                        || (taskVariableNode.getNodeType() == Node.COMMENT_NODE))
                                    continue;
                                
                                value = taskVariableNode.getNodeName();
                                if (!Utils.isNothing(value))
                                {
                                    Variable var = new Variable();
                                    
                                    var.setName(value);
                                    
                                    var.setValue(xml.getStringAttribute(
                                            taskVariableNode, VAR_VALUE));
                                    
                                    parseVariableAttrs(xml, taskVariableNode,
                                            var);
                                    
                                    variables.put(var.getName(), var);
                                }
                            }
                            
                            if (variables.size() > 0)
                                task.setVariables(variables);
                        }
                    }
                    
                    task.parseScope(xml
                            .getStringAttribute(taskNode, TASK_SCOPE));
                    
                    OnTask onTask = (OnTask)ObjectFactory.instance().get(
                            task.getClassName(), true);
                    
                    if (onTask == null)
                        continue;
                    
                    tasks.put(task.getName(), task);
                    
                    if (onTask.isInlineTask() || task.isScopeSet(Task.INLINE))
                        inlineTasks.put(task.getName(), task);
                    
                    if (onTask.isPostTask() || task.isScopeSet(Task.POST))
                        postTasks.put(task.getName(), task);
                    
                    if (onTask.isPreEtlTask()
                            || task.isScopeSet(Task.BEFORE_ETL))
                        preEtlTasks.put(task.getName(), task);
                    
                }
                
                block.setTasks(tasks);
                
                if (inlineTasks.size() > 0)
                    block.setInlineTasks(inlineTasks);
                
                if (postTasks.size() > 0)
                    block.setPostTasks(postTasks);
                
                if (preEtlTasks.size() > 0)
                    block.setBeforeEtlTasks(preEtlTasks);
            }
            
        }
    }
    
    /**
     * Parses the variable attributes.
     * 
     * @param xml
     *            the xml dom
     * @param variableNode
     *            the variable node
     * @param var
     *            the variable
     */
    public void parseVariableAttrs(XmlUtils xml, Node variableNode, Variable var)
    {
        if (xml == null || variableNode == null || var == null)
            return;
        
        var.setAttrs(xml.getAttributes(variableNode));
    }
    
    /**
     * Parses the variable object.
     * 
     * @param xml
     *            the xml dom
     * @param variableNode
     *            the variable node
     * @param var
     *            the variable
     */
    public void parseVariableObject(XmlUtils xml, Node variableNode,
            Variable var)
    {
        
    }
    
}
