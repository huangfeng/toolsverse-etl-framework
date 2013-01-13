/*
 * Destination.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.toolsverse.cache.Cache;
import com.toolsverse.cache.CacheProvider;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.OnException;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * <code>Destination</code> is a one of the two building blocks of the ETL
 * scenario: etl process extracts data from the Sources and loads into the
 * Destinations. It implements <code>Block</code> interface and adds some
 * methods and members specific for the Destination. There are can be multiple
 * destinations in the scenario, each with its own <code>DataSet</code> and
 * linked to the different <code>Source</code>. Also it is possible to have
 * destination without source, and special type of destination such as database
 * stored procedure or function. In some cases the code for the destination can
 * be executed in the separate thread.
 * 
 * <p>
 * <b>Example</b> of destination declaration in the scenario file:</br>
 * <blockquote>
 * <dt>{@code <destination>}</dt>
 * <dd>{@code <name>image</name>}</dt>
 * </br>
 * <dd>{@code <source>image</source>}</dt>
 * </br>
 * <dd>{@code <load>}</dt>
 * </br>
 * <dd>{@code <connection>mssql</connection>}</dt>
 * </br>
 * <dd>{@code <driver>com.toolsverse.etl.driver.mysql.MsSqlDriver</driver>}</dt>
 * </br>
 * <dd>{@code </load>}</dt>
 * </br>
 * <dt>{@code </destination>}</dt>
 * </blockquote>
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class Destination extends OnException implements Block,
        CacheProvider<String, Object>
{
    
    /** The global scope. */
    public static final String SCOPE_GLOBAL = "global";
    
    /** The single scope. */
    public static final String SCOPE_SINGLE = "single";
    
    /** Destination with data set. Basically the destination which represents some
     * data. */
    public static String REGULAR_TYPE = "regular";
    
    /** Database stored procedure. */
    public static String PROC_TYPE = "procedure";
    
    /** Database function. */
    public static String FUNC_TYPE = "function";
    
    /** DDL (data definition) sql. */
    public static String DDL_TYPE = "ddl";
    
    /** The empty destination which does nothing except all running destination
     * threads have to finish before this one.. */
    public static String WAIT_TYPE = "wait";
    
    /** Database table. */
    public static String TABLE_TYPE = "table";
    
    // what to do when work is done for the destination
    
    /**
     * If destination uses database cursor to load data and cursor is created
     * from the table, previously populated during extract ON_FINISH_KEEP means
     * table should not be dropped right after etl processed finished with the
     * destination.
     */
    public static final int ON_FINISH_KEEP = 0;
    
    /**
     * If destination uses database cursor to load data and cursor is created
     * from the table, previously populated during extract ON_FINISH_DROP
     * means table should be dropped right after etl processed finished with the
     * destination.
     */
    public static final int ON_FINISH_DROP = 1;
    
    /** The ON_FINISH_KEEP code. */
    public static final String ON_FINISH_KEEP_STR = "keep";
    
    /** The ON_FINISH_DROP code. */
    public static final String ON_FINISH_DROP_STR = "drop";
    
    /** The ON_FINISH. */
    public static final Map<String, Integer> ON_FINISH = new HashMap<String, Integer>();
    
    /** types of the table which is used to create cursor. */
    public static String CURSOR_TABLE_TEMPORARY_TYPE = "temporary";
    
    /** The temporary table is used to created cursor. */
    public static String CURSOR_TABLE_TEMP_TYPE = "temp";
    
    /** The regulr table is used to created cursor. */
    public static String CURSOR_TABLE_REGULAR_TYPE = "regular";
    static
    {
        ON_FINISH.put(ON_FINISH_KEEP_STR, new Integer(ON_FINISH_KEEP));
        ON_FINISH.put(ON_FINISH_DROP_STR, new Integer(ON_FINISH_DROP));
    }
    
    /** The INSERT action. */
    public static final String LOAD_INSERT = "insert";
    
    /** The MERGE action. */
    public static final String LOAD_MERGE = "merge";
    
    /** The LOAD action. */
    public static final String LOAD_UPDATE = "update";
    
    /** The DELETE action. */
    public static final String LOAD_DELETE = "delete";
    
    /** The LOAD_ACTIONS. */
    public static final Set<String> LOAD_ACTIONS = new HashSet<String>();
    static
    {
        LOAD_ACTIONS.add(LOAD_INSERT);
        LOAD_ACTIONS.add(LOAD_MERGE);
        LOAD_ACTIONS.add(LOAD_UPDATE);
        LOAD_ACTIONS.add(LOAD_DELETE);
    }
    
    /** The name. */
    private String _name;
    
    /** The sql. */
    private String _sql;
    
    /** The source. */
    private Source _source;
    
    /** The encode flag. */
    private boolean _encode;
    
    /** The object name. */
    private String _objectName;
    
    /** The condition. */
    private String _condition;
    
    /** The then for the condition. */
    private String _then;
    
    /** The else for the condition. */
    private String _else;
    
    /** The after for the condition. */
    private String _after;
    
    /** The variables. */
    private ListHashMap<String, Variable> _variables;
    
    /** The data set. */
    private DataSet _dataSet;
    
    /** The empty flag. */
    private boolean _empty;
    
    /** The destination type. */
    private String _type;
    
    /** The enabled flag. */
    private boolean _enabled;
    
    /** The data reader class name. */
    private String _dataReaderClassName;
    
    /** The data writer class name. */
    private String _dataWriterClassName;
    
    /** The data reader params. */
    private Map<String, String> _dataReaderParams;
    
    /** The data writer params. */
    private Map<String, String> _dataWriterParams;
    
    /** The parallel flag. */
    private boolean _parallel;
    
    /** The scenario variables. */
    private ListHashMap<String, Variable> _scenarioVariables;
    
    /** The metadata extractor class. */
    private String _metadataExtractorClass;
    
    /** The use metadata data types flag. */
    private boolean _useMetadataDataTypes;
    
    /** The create indexes from the source flag. */
    private boolean _createIndexes;
    
    /** The suffix which will be added to the index when it is created from the source. */
    private String _indexSuffix;
    
    /** The cache. */
    private Cache<String, Object> _cache;
    
    /** The cursor table name. */
    private String _cursorTableName;
    
    /** The cursor sql. */
    private String _cursorSql;
    
    /** The on finish. */
    private int _onFinish;
    
    /** The cursor table type. */
    private String _cursorTableType;
    
    /** The is stream flag. */
    private boolean _isStream;
    
    /** The connection name. */
    private String _connectionName;
    
    /** The driver class name. */
    private String _driverClassName;
    
    /** The no connection flag. */
    private boolean _noConnection;
    
    /** The tasks. */
    private ListHashMap<String, Task> _tasks;
    
    /** The post tasks. */
    private ListHashMap<String, Task> _postTasks;
    
    /** The inline tasks. */
    private ListHashMap<String, Task> _inlineTasks;
    
    /** The pre etl tasks. */
    private ListHashMap<String, Task> _preEtlTasks;
    
    /** The scope. */
    private String _scope;
    
    /** The load action. */
    private String _loadAction;
    
    /** The load key. */
    private String _loadKey;
    
    /** The metadata. */
    private Map<String, FieldDef> _metadata;
    
    /**
     * Instantiates a new destination.
     */
    public Destination()
    {
        super();
        
        _name = null;
        _sql = null;
        _source = null;
        _encode = true;
        _objectName = null;
        _condition = null;
        _then = null;
        _else = null;
        _after = null;
        _variables = new ListHashMap<String, Variable>();
        _dataSet = null;
        _empty = false;
        _isStream = false;
        _type = null;
        _enabled = true;
        _dataReaderClassName = null;
        _dataWriterClassName = null;
        _dataReaderParams = null;
        _dataWriterParams = null;
        _parallel = false;
        _scenarioVariables = null;
        _metadataExtractorClass = null;
        _useMetadataDataTypes = true;
        _createIndexes = false;
        _indexSuffix = null;
        _cache = null;
        _cursorTableName = null;
        _cursorSql = null;
        _onFinish = ON_FINISH_KEEP;
        _cursorTableType = CURSOR_TABLE_REGULAR_TYPE;
        
        _noConnection = false;
        _connectionName = null;
        _driverClassName = null;
        
        _tasks = null;
        _postTasks = null;
        _inlineTasks = null;
        _preEtlTasks = null;
        
        _scope = SCOPE_GLOBAL;
        
        _loadAction = LOAD_INSERT;
        _loadKey = null;
    }
    
    /**
     * Gets the block of code which is executed after all auto generated and
     * conditional blocks of the destination. For example there are blocks
     * "condition", "then" and "else". ETL framework will generated something
     * like <code>if something then then_code else else_code end if;</code>
     * The "after" block will be executed after latest "end if;"
     * 
     * @return the block of code which is executed after
     *         <code>if something then then_code else else_code end if;</code>
     */
    public String getAfter()
    {
        return _after;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#getBeforeEtlTasks()
     */
    public ListHashMap<String, Task> getBeforeEtlTasks()
    {
        return _preEtlTasks;
    }
    
    /**
     * Gets the cache associated with the destination. Cache can be used by the
     * external functions.
     * 
     * @return the cache
     */
    public Cache<String, Object> getCache()
    {
        return _cache;
    }
    
    /**
     * Gets the condition string, usually sql, executed before the code for the
     * destination. The simplest example would be "if" condition:
     * <code>something is not null</code>. The etl framework generates code
     * which splits sql on the blocks so the actual code for the destination
     * (for example insert, update or delete) will be executed only if condition
     * applies.
     * 
     * @return the condition
     */
    public String getCondition()
    {
        return _condition;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#getConnection()
     */
    public Connection getConnection()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#getConnectionName()
     */
    public String getConnectionName()
    {
        return _connectionName;
    }
    
    /**
     * Gets the cursor sql.
     *
     * @return the cursor sql
     */
    public String getCursorSql()
    {
        return _cursorSql;
    }
    
    /**
     * Gets the name of source table for the cursor. This name is used to open
     * and fetch data from the cursor for the load.
     * 
     * @return the name of the source table
     */
    public String getCursorTableName()
    {
        return _cursorTableName;
    }
    
    /**
     * Gets the name of the source table for the cursor using database specific semantic
     * implemented in the <code>Driver</code>.
     *
     * @param driver The driver
     * @return the name of the source table
     */
    public String getCursorTableName(Driver driver)
    {
        if (isCursorTableTemp() && !Utils.isNothing(_cursorTableName))
            return driver.getTempTableName(_cursorTableName);
        else
            return _cursorTableName;
    }
    
    /**
     * Gets the type of the source table for the cursor. Can be either temporary or permanent.
     * The table is used to open cursor and fetch data for the load from from
     * it. The default is temporary if not set otherwise.
     * 
     * @return the type of the source table
     */
    public String getCursorTableType()
    {
        if (Utils.isNothing(_cursorTableType))
            return CURSOR_TABLE_TEMP_TYPE;
        
        return _cursorTableType;
    }
    
    /**
     * Gets the class name of the data reader.
     * 
     * @return the class name of the data reader
     */
    public String getDataReaderClassName()
    {
        return _dataReaderClassName;
    }
    
    /**
     * Gets the data reader params.
     *
     * @return the data reader params
     */
    public Map<String, String> getDataReaderParams()
    {
        return _dataReaderParams;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#getDataSet()
     */
    public DataSet getDataSet()
    {
        return _dataSet;
    }
    
    /**
     * Gets the data writer class name.
     *
     * @return the data writer class name
     */
    public String getDataWriterClassName()
    {
        return _dataWriterClassName;
    }
    
    /**
     * Gets the data writer params.
     *
     * @return the data writer params
     */
    public Map<String, String> getDataWriterParams()
    {
        return _dataWriterParams;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.ConditionalExecution#getDefaultConnectionName
     * ()
     */
    @Override
    public String getDefaultConnectionName()
    {
        return !Utils.isNothing(_connectionName) ? _connectionName
                : EtlConfig.DEST_CONNECTION_NAME;
    }
    
    /**
     * Gets the class name of the driver. Driver is used to generate database
     * specific SQL.
     * 
     * @return the driver class name
     */
    public String getDriverClassName()
    {
        return _driverClassName;
    }
    
    /**
     * Gets the block of code which is executed after "else" in the
     * <code>if something then then_code else else_code end if;</code>.
     *
     * @return the else
     */
    public String getElse()
    {
        return _else;
    }
    
    /**
     * Gets the index suffix. It will be added to the index name when index is created from the source.
     *
     * @return the index suffix
     */
    public String getIndexSuffix()
    {
        return _indexSuffix;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#getInlineTasks()
     */
    public ListHashMap<String, Task> getInlineTasks()
    {
        return _inlineTasks;
    }
    
    /**
     * Gets the load action.
     *
     * @return the load action
     */
    public String getLoadAction()
    {
        return _loadAction;
    }
    
    /**
     * Gets the load key.
     *
     * @return the load key
     */
    public String getLoadKey()
    {
        return _loadKey;
    }
    
    /**
     * Gets the meta data.
     *
     * @return the meta data
     */
    public Map<String, FieldDef> getMetaData()
    {
        return _metadata;
    }
    
    /**
     * Gets the metadata extractor class. Meta data automatically populated
     * during the load if metadata class name is specified at the scenario or
     * destination level.
     * 
     * @return the metadata extractor class name
     */
    public String getMetadataExtractorClass()
    {
        return _metadataExtractorClass;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.common.OnException#getName()
     */
    @Override
    public String getName()
    {
        return _name;
    }
    
    /**
     * Gets the name of the object. Can be a table name or stored procedure or
     * function name. It used by registered code generator to create database
     * specific load code for the destination.
     * 
     * @return the object name
     */
    public String getObjectName()
    {
        return _objectName;
    }
    
    /**
     * Gets the type of action on finish (when etl process finished processing
     * destination). Possible actions are: ON_FINISH_KEEP, ON_FINISH_DROP.
     * 
     * @return the type of action on finish
     */
    public int getOnFinish()
    {
        return _onFinish;
    }
    
    /**
     * Parses string value of the type of action on finish.
     * 
     * @param action
     *                The action
     * 
     * @return the type of action on finish
     */
    public int getOnFinish(String action)
    {
        if (action == null)
            return ON_FINISH_KEEP;
        
        Integer value = ON_FINISH.get(action.toLowerCase());
        
        if (value == null)
            return ON_FINISH_KEEP;
        
        return value.intValue();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#getPostTasks()
     */
    public ListHashMap<String, Task> getPostTasks()
    {
        return _postTasks;
    }
    
    /**
     * Gets the variables linked to the scenario.
     * 
     * @return the scenario variables
     */
    public ListHashMap<String, Variable> getScenarioVariables()
    {
        return _scenarioVariables;
    }
    
    /**
     * Gets the scope.
     *
     * @return the scope
     */
    public String getScope()
    {
        return _scope;
    }
    
    /**
     * Gets the source, linked to the destination. It is possible to have a
     * destination without source.
     * 
     * @return the source
     */
    public Source getSource()
    {
        return _source;
    }
    
    /**
     * Gets the sql for the load. By default etl framework auto generates code
     * for the destination, using associated driver but it is possible to add it
     * manually. Code will be merged with the variables.
     * 
     * @return the sql
     */
    public String getSql()
    {
        return _sql;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#getTasks()
     */
    public ListHashMap<String, Task> getTasks()
    {
        return _tasks;
    }
    
    /**
     * Gets the block of code which is executed after "then" in the
     * <code>if something then then_code else else_code end if;</code>.
     *
     * @return the block of code executed after "then"
     */
    public String getThen()
    {
        return _then;
    }
    
    /**
     * Gets the type of the Destination. The possible types are: REGULAR_TYPE,
     * PROC_TYPE, FUNC_TYPE, DDL_TYPE, WAIT_TYPE, TABLE_TYPE.
     * 
     * @return the type
     */
    public String getType()
    {
        if (Utils.isNothing(_type))
            return REGULAR_TYPE;
        
        return _type;
    }
    
    /**
     * Gets the "use the metadata data types" flag. If <code>true</code> the
     * dataset data types inherited by the Destination from the Source will be
     * replaced on data types returned by <code>MetadataExtractor</code>
     * 
     * @return the "use metadata data types" flag
     */
    public boolean getUseMetadataDataTypes()
    {
        return _useMetadataDataTypes;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#getVariable(java.lang.String)
     */
    public Variable getVariable(String name)
    {
        if (_variables == null)
            return null;
        
        return _variables.get(name);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#getVariables()
     */
    public ListHashMap<String, Variable> getVariables()
    {
        return _variables;
    }
    
    /**
     * Checks if indexes need to be created from the source when creating a table.
     *
     * @return true, if indexes need to be created from the source when creating a table
     */
    public boolean isCreateIndexes()
    {
        return _createIndexes;
    }
    
    /**
     * Checks if the source table for the cursor is temporary.
     * 
     * @return true, if is the source table for the cursor is temporary
     */
    public boolean isCursorTableTemp()
    {
        return CURSOR_TABLE_TEMP_TYPE.equalsIgnoreCase(_cursorTableType);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#isEmpty()
     */
    public boolean isEmpty()
    {
        return _empty;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#isEnabled()
     */
    public boolean isEnabled()
    {
        return _enabled;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#isEncoded()
     */
    public boolean isEncoded()
    {
        return _encode;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#isParallel()
     */
    public boolean isParallel()
    {
        return _parallel;
    }
    
    /**
     * Checks if is destination is a stored procedure or function.
     * 
     * @return true, if destination is a stored procedure or function
     */
    public boolean isProcOrFunc()
    {
        return PROC_TYPE.equalsIgnoreCase(getType())
                || FUNC_TYPE.equalsIgnoreCase(getType());
    }
    
    /**
     * Checks if data should be streamed from the source to the destination. If true the memory will be allocated only for the current row.
     *
     * @return true, if data should be streamed from the source to the destination.
     */
    public boolean isStream()
    {
        return _isStream;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#noConnection()
     */
    public boolean noConnection()
    {
        return _noConnection;
    }
    
    /**
     * Sets the block of code which is executed after all auto generated and
     * conditional blocks of the destination.
     * 
     * <p>
     * Example of the <code>"after"</code>:
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <load>}</dt>
     * </br>
     * <dd>{@code <condition>(v_NAME IS NOT NULL and v_count = 0)</condition>}</dt>
     * </br>
     * <dd>{@code <after>v_CUSTOMER_NUM:= 1;}</dt>
     * </br>
     * <dd>{@code </after>}</dt>
     * </br>
     * <dd>{@code </load>}</dt>
     * </br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new block of code which is executed after all auto
     *                generated and conditional blocks of the destination
     */
    public void setAfter(String value)
    {
        _after = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.Block#setBeforeEtlTasks(com.toolsverse
     * .util.ListHashMap)
     */
    public void setBeforeEtlTasks(ListHashMap<String, Task> value)
    {
        _preEtlTasks = value;
    }
    
    /**
     * Sets the cache associated with the destination.
     * 
     * @param value
     *                The new cache
     * 
     */
    public void setCache(Cache<String, Object> value)
    {
        _cache = value;
    }
    
    /**
     * Sets the condition, usually sql, executed before the code for the
     * destination.
     * 
     * <p>
     * Example of the <code>condition</code>. In this example auto generated
     * code for the destination, most probably "insert into some table" will be
     * executed only if condition
     * <code>v_NAME IS NOT NULL and v_count = 0</code> applies: <blockquote>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <load>}</dt>
     * </br>
     * <dd>{@code <condition>(v_NAME IS NOT NULL and v_count = 0)</condition>}</dt>
     * </br>
     * <dd>{@code </load>}</dt>
     * </br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                the new condition
     */
    public void setCondition(String value)
    {
        _condition = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.Block#setConnection(java.sql.Connection)
     */
    public void setConnection(Connection value)
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.Block#setConnectionName(java.lang.String)
     */
    public void setConnectionName(String value)
    {
        _connectionName = value;
    }
    
    /**
     * Sets the value of the "create indexes when creating a table from the source" flag.
     *
     * @param value the new value of the "create indexes when creating a table from the source" flag
     */
    public void setCreateIndexes(boolean value)
    {
        _createIndexes = value;
    }
    
    /**
     * Sets the cursor sql.
     *
     * @param value the new cursor sql
     */
    public void setCursorSql(String value)
    {
        _cursorSql = value;
    }
    
    /**
     * Sets the name of the source table for the cursor.
     * 
     * <p>
     * Example of the setting of the name of the source table at the destination
     * level in the scenario file: 
     * <blockquote>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <sourcetable name="tmp_table" onfinish="keep" />}</br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new name of the source table
     */
    public void setCursorTableName(String value)
    {
        _cursorTableName = value;
    }
    
    /**
     * Sets the type of the source table for the cursor.
     * 
     * <p>
     * Example of the setting the type of the source table in the scenario file:
     * <blockquote>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <sourcetable name="tmp_table" onfinish="delete" type="regular" />}</br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new type of the source table
     */
    public void setCursorTableType(String value)
    {
        if (CURSOR_TABLE_TEMPORARY_TYPE.equalsIgnoreCase(value)
                || CURSOR_TABLE_TEMP_TYPE.equalsIgnoreCase(value))
            _cursorTableType = CURSOR_TABLE_TEMP_TYPE;
        else
            _cursorTableType = CURSOR_TABLE_REGULAR_TYPE;
    }
    
    /**
     * Sets the class name of the data reader. Data reader must implement DataSetConnector interface.
     * 
     * <p>
     * Example of the <code>DataReader</code> at the destination level:
     * <blockquote>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <load>}</dt>
     * </br>
     * <dd>{@code <reader>com.toolsverse.etl.connector.text.TextConnector</reader>}</dt>
     * </br>
     * <dd>{@code </load>}</dt>
     * </br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new class name of the data reader.
     * 
     */
    public void setDataReaderClassName(String value)
    {
        if (EtlConfig.NONE.equalsIgnoreCase(value))
            _dataReaderClassName = null;
        else
            _dataReaderClassName = value;
    }
    
    /**
     * Sets the data reader params.
     *
     * @param value the value
     */
    public void setDataReaderParams(Map<String, String> value)
    {
        _dataReaderParams = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.Block#setDataSet(com.toolsverse.etl.common
     * .DataSet)
     */
    public void setDataSet(DataSet value)
    {
        _dataSet = value;
        
        if (_dataSet == null)
            return;
        
        _dataSet.setVariables(getVariables());
        
        updateFields(_dataSet.getDriver());
    }
    
    /**
     * Sets the data writer class name. Data writer must implement DataSetConnector interface.
     *
     * @param value the new data writer class name
     */
    public void setDataWriterClassName(String value)
    {
        if (EtlConfig.NONE.equalsIgnoreCase(value))
            _dataWriterClassName = null;
        else
            _dataWriterClassName = value;
    }
    
    /**
     * Sets the data writer params.
     *
     * @param value the value
     */
    public void setDataWriterParams(Map<String, String> value)
    {
        _dataWriterParams = value;
    }
    
    /**
     * Sets the class name of the driver. Driver is used to generate database
     * specific SQL.
     * 
     * <p>
     * <b>Example:</b> <blockquote>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>image</name>}</dt>
     * </br>
     * <dd>{@code <source>image</source>}</dt>
     * </br>
     * <dd>{@code <load>}</dt>
     * </br>
     * <dd>{@code <driver>com.toolsverse.etl.driver.sqlserver.MsSqlDriver</driver>}</dt>
     * </br>
     * <dd>{@code </load>}</dt>
     * </br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new class name of the driver
     * 
     */
    public void setDriverClassName(String value)
    {
        _driverClassName = value;
    }
    
    /**
     * Sets the block of code which is executed after "else" in the
     * <code>if something then then_code else else_code end if;</code>
     * 
     * <p>
     * Example of the <code>"else"</code>. In this example if condition
     * <code>v_NAME IS NOT NULL and v_count = 0</code> does not applie the
     * code <code>v_CUSTOMER_NUM:= 1;</code> will be executed. <blockquote>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <load>}</dt>
     * </br>
     * <dd>{@code <condition>(v_NAME IS NOT NULL and v_count = 0)</condition>}</dt>
     * </br>
     * <dd>{@code <else>v_CUSTOMER_NUM:= 1;}</dt>
     * </br>
     * <dd>{@code </else>}</dt>
     * </br>
     * <dd>{@code </load>}</dt>
     * </br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new block of code which is executed after "else"
     */
    public void setElse(String value)
    {
        _else = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#setEnabled(boolean)
     */
    public void setEnabled(boolean value)
    {
        _enabled = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#setEncoded(boolean)
     */
    public void setEncoded(boolean value)
    {
        _encode = value;
    }
    
    /**
     * Sets the index suffix. It will be added to the index name when index is created from the source.
     *
     * @param value the new index suffix
     */
    public void setIndexSuffix(String value)
    {
        _indexSuffix = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.Block#setInlineTasks(com.toolsverse.util
     * .ListHashMap)
     */
    public void setInlineTasks(ListHashMap<String, Task> value)
    {
        _inlineTasks = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#setIsEmpty(boolean)
     */
    public void setIsEmpty(boolean value)
    {
        _empty = value;
    }
    
    /**
     * Sets the load action. Possible action: LOAD_INSERT, LOAD_UPDATE, LOAD_DELETE, LOAD_MERGE
     *
     * @param value the new load action
     */
    public void setLoadAction(String value)
    {
        String action = Utils.makeString(value).toLowerCase().trim();
        
        if (LOAD_ACTIONS.contains(action))
            _loadAction = action;
    }
    
    /**
     * Sets the load key.
     *
     * @param value the new load key
     */
    public void setLoadKey(String value)
    {
        _loadKey = value;
    }
    
    /**
     * Sets the meta data.
     *
     * @param value the value
     */
    public void setMetaData(Map<String, FieldDef> value)
    {
        _metadata = value;
    }
    
    /**
     * Sets the metadata extractor class name.
     * 
     * <p>
     * <b>Example</b>:</br>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <metadata class="com.toolsverse.etl.metadata.GenericMetadataExtractor" />}</dt>
     * </br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new metadata extractor class
     */
    public void setMetadataExtractorClass(String value)
    {
        _metadataExtractorClass = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.common.OnException#setName(java.lang.String)
     */
    @Override
    public void setName(String value)
    {
        _name = value;
    }
    
    /**
     * Sets the "no connection" flag. If true no connection will be created for the destination.
     *
     * @param value the new "no connection" flag
     */
    public void setNoConnection(boolean value)
    {
        _noConnection = value;
    }
    
    /**
     * Sets the name of the object.
     * 
     * <p>
     * Example of the <code>objectname</code> at the destination level in the
     * scenario file: <blockquote>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <objectname>CLIENT</objectname>}</dt>
     * </br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new name of the object
     */
    public void setObjectName(String value)
    {
        _objectName = value;
    }
    
    /**
     * Sets the type of action on finish (when etl process finished processing
     * destination). Possible actions are: ON_FINISH_KEEP, ON_FINISH_DROP. The
     * default action is ON_FINISH_DROP.
     * 
     * <p>
     * Example of the setting of the type of action on finish in the scenario
     * file: <blockquote>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <sourcetable name="tmp_table" onfinish="delete" />}</br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new type of action on finish
     */
    public void setOnFinish(int value)
    {
        _onFinish = value;
    }
    
    /**
     * Sets the the type of action on finish based on string value.
     * 
     * @param action
     *                The new type of action on finish
     */
    public void setOnFinish(String action)
    {
        _onFinish = getOnFinish(action);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#setParallel(boolean)
     */
    public void setParallel(boolean value)
    {
        _parallel = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.Block#setPostTasks(com.toolsverse.util
     * .ListHashMap)
     */
    public void setPostTasks(ListHashMap<String, Task> value)
    {
        _postTasks = value;
    }
    
    /**
     * Sets the variables which belongs to the scenario.
     * 
     * <p>
     * <b>Example</b> of the scenario variables:</br> <blockquote>
     * <dt>{@code <scenario>}</dd>
     * </br>
     * <dd>{@code <name>test</name>}</dd>
     * </br>
     * <dd>{@code <variables>}</dd>
     * </br>
     * <dd>{@code <CUSTOMER value="test1" />}</dd>
     * </br>
     * <dd>{@code <PRODUCT value="test2" />}</dd>
     * </br>
     * <dd>{@code </variables>}</dd>
     * </br> </blockquote>
     * 
     * @param value
     *                The new scenario variables
     */
    public void setScenarioVariables(ListHashMap<String, Variable> value)
    {
        _scenarioVariables = value;
    }
    
    /**
     * Sets the scope. Possible values: SCOPE_GLOBAL - code for the destination will be included in the code for the scenario, SCOPE_SINGLE - code for the 
     * destination will be executed separatelly. 
     *
     * @param value the new scope
     */
    public void setScope(String value)
    {
        if (Utils.isNothing(value)
                || (!SCOPE_GLOBAL.equalsIgnoreCase(value) && !SCOPE_SINGLE
                        .equalsIgnoreCase(value)))
            _scope = SCOPE_GLOBAL;
        else
            _scope = value.toLowerCase();
    }
    
    /**
     * Sets the source, linked to the destination. Usually it is done by
     * referencing source by name. By default the etl framework is trying to
     * find source with the same name as destination.
     * 
     * <p>
     * Example:
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <source>test_source</source>}</dt>
     * </br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new source
     */
    public void setSource(Source value)
    {
        _source = value;
    }
    
    /**
     * Sets the sql for the load.
     * 
     * <p>
     * <b>Example:</b>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <load>}</dt>
     * </br>
     * <dd>{@code <sql>insert into table1 (field1, field2) values({VAR1},
     * {VAR2});}</dt>
     * </br>
     * <dd>{@code </sql>}</dt>
     * </br>
     * <dd>{@code </load>}</dt>
     * </br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new sql
     */
    public void setSql(String value)
    {
        _sql = value;
    }
    
    /**
     * Sets the "stream" flag. If true the data from the source to the destination will be streamed. 
     *
     * @param value the new "stream" flag
     */
    public void setStream(boolean value)
    {
        _isStream = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.Block#setTasks(com.toolsverse.util.ListHashMap
     * )
     */
    public void setTasks(ListHashMap<String, Task> value)
    {
        _tasks = value;
    }
    
    /**
     * Sets the block of code which is executed after "then" in the
     * <code>if something then then_code else else_code end if;</code> It is
     * important to understand that the auto generated code, for example "insert
     * into" will be executed in the same block as "then".
     * 
     * <p>
     * Example of the <code>"then"</code>. In this example if condition
     * <code>v_NAME IS NOT NULL and v_count = 0</code> applies the code which
     * starts from <code>EXECUTE IMMEDIATE</code> will be executed.
     * <blockquote>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <load>}</dt>
     * </br>
     * <dd>{@code <condition>(v_NAME IS NOT NULL and v_count = 0)</condition>}</dt>
     * </br>
     * <dd>{@code <then>EXECUTE IMMEDIATE 'insert into primary_keys_table (field_name, pk, old_pk) values(''CUSTOMER_NUM'',:C_NUM, :CUSTOMER_NUM)'}</dt>
     * </br>
     * <dd>{@code USING v_C_NUM, v_CUSTOMER_NUM;}</dt>
     * </br>
     * <dd>{@code v_CUSTOMER_NUM:= v_C_NUM;}</dt>
     * </br>
     * <dd>{@code </then>}</dt>
     * </br>
     * <dd>{@code </load>}</dt>
     * </br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new block of code executed after "then"
     */
    public void setThen(String value)
    {
        _then = value;
    }
    
    /**
     * Sets the type of the destination.
     * 
     * <p>
     * <b>Example</b> of destination with the FUNCTION type in the scenario
     * file:</br> <blockquote>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <driver>com.toolsverse.etl.driver.sqlserver.MsSqlDriver</driver>}</dt>
     * </br>
     * <dd>{@code <sql>}</dt>
     * </br>
     * <dd>{@code CREATE FUNCTION convert_value(@value VARCHAR(3000)) RETURNS VARCHAR(3000)}</dt>
     * <dd>{@code AS}</dt>
     * <dd>{@code BEGIN}</dt>
     * <dd>{@code return @value;}</dt>
     * <dd>{@code END;}</dt>
     * <dd>{@code </sql>}</dt>
     * </br>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * @param value
     *                The new type
     * 
     */
    public void setType(String value)
    {
        _type = value;
    }
    
    /**
     * Sets the "use the metadata data types" flag.
     * 
     * @param value
     *                The new value for the "use metadata data types" flag
     */
    public void setUseMetadataDataTypes(boolean value)
    {
        _useMetadataDataTypes = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.Block#setVariables(com.toolsverse.util
     * .ListHashMap)
     */
    public void setVariables(ListHashMap<String, Variable> value)
    {
        _variables = value;
    }
    
    /**
     * Updates fields visibility based on variable.isInclude() flag. Also sets sql and native data types if variable
     * attributes {@link Variable#NATIVE_TYPE_ATTR} or {@link Variable#SQL_TYPE_ATTR} are set.
     * If field is not visible it will not be included into the insert/update/merge actions.
     *
     * @param driver the driver
     */
    public void updateFields(Driver driver)
    {
        if (getVariables() != null && getVariables().size() > 0
                && _dataSet != null)
        {
            for (int i = 0; i < getVariables().size(); i++)
            {
                Variable var = getVariables().get(i);
                
                FieldDef field = _dataSet.getFieldDef(var.getName());
                
                if (field == null && !Utils.isNothing(var.getFieldName()))
                    field = _dataSet.getFieldDef(var.getFieldName());
                
                if (field != null)
                {
                    field.setVisible(var.isInclude());
                    
                    if (!var.getName().equalsIgnoreCase(field.getName()))
                    {
                        field.setName(var.getName());
                    }
                    
                    if (driver == null)
                        continue;
                    
                    String nativetype = var
                            .getAttrValue(Variable.NATIVE_TYPE_ATTR);
                    Integer sqltype = Utils.str2Integer(
                            var.getAttrValue(Variable.SQL_TYPE_ATTR), null);
                    
                    if (!Utils.isNothing(nativetype) && sqltype == null)
                    {
                        sqltype = SqlUtils.getTypeByName(nativetype);
                    }
                    else if (sqltype != null && Utils.isNothing(nativetype))
                    {
                        nativetype = SqlUtils.getTypeName(sqltype.intValue());
                    }
                    
                    if (!Utils.isNothing(nativetype) && sqltype != null)
                    {
                        field.setNativeDataType(nativetype);
                        field.setSqlDataType(sqltype);
                        
                        TypedKeyValue<Integer, Integer> precScale = SqlUtils
                                .getScaleAndPrecision(nativetype);
                        
                        if (precScale != null && precScale.getKey() != null)
                        {
                            field.setPrecision(precScale.getKey());
                            field.setFieldSize(precScale.getKey());
                            if (precScale.getValue() != null)
                                field.setScale(precScale.getValue());
                        }
                    }
                    
                }
            }
        }
    }
}
