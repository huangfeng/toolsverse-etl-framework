/*
 * Source.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.sql.Connection;
import java.util.Map;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.OnException;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.Utils;

/**
 * <code>Source</code> is a one of the two building blocks of the ETL
 * scenario: etl process extracts data from the Sources and loads into the
 * Destinations. It implements <code>Block</code> interface and adds some
 * methods and members specific for the Source. There are can be multiple
 * sources in the scenario. In some cases the code for the source can
 * be executed in the separate thread.
 * 
 * <p>
 * <b>Example</b> of source declaration in the scenario file:</br>
 * <blockquote>
 * <dt>{@code <source>}</dt>
 * <dd>{@code <name>image</name>}</dt>
 * </br>
 * <dd>{@code <extract>}</dt>
 * </br>
 * <dd>{@code <sql>select * from image</sql>}</dt>
 * </br>
 * <dd>{@code </extract>}</dt>
 * </br>
 * <dt>{@code </source>}</dt>
 * </blockquote>
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class Source extends OnException implements Block
{
    
    /** The name. */
    private String _name;
    
    /** The sql. */
    private String _sql;
    
    /** The using. */
    private String _using;
    
    /** The variables. */
    private ListHashMap<String, Variable> _variables;
    
    /** The tasks. */
    private ListHashMap<String, Task> _tasks;
    
    /** The post tasks. */
    private ListHashMap<String, Task> _postTasks;
    
    /** The inline tasks. */
    private ListHashMap<String, Task> _inlineTasks;
    
    /** The before etl tasks. */
    private ListHashMap<String, Task> _beforeEtlTasks;
    
    /** The data set. */
    private DataSet _dataSet;
    
    /** The object name. */
    private String _objectName;
    
    /** The "no connection" flag. */
    private boolean _noConnection;
    
    /** The connection name. */
    private String _connectionName;
    
    /** The connection. */
    transient private Connection _connection;
    
    /** The independent flag. */
    private Boolean _independent;
    
    /** The empty flag. */
    private boolean _empty;
    
    /** The encode flag. */
    private boolean _encode;
    
    /** The enabled flag. */
    private boolean _enabled;
    
    /** The parallel flag. */
    private boolean _parallel;
    
    /** The mandatory flag. */
    private boolean _mandatory;
    
    /** The data writer class name. */
    private String _dataWriterClassName;
    
    /** The data reader class name. */
    private String _dataReaderClassName;
    
    /** The data writer params. */
    private Map<String, String> _dataWriterParams;
    
    /** The data reader params. */
    private Map<String, String> _dataReaderParams;
    
    /** The usage counter. */
    private int _usageCounter;
    
    /** The key name. */
    private String _keyName;
    
    /** The key field. */
    private String _keyField;
    
    /** The driver class name. */
    private String _driverClassName;
    
    /** The on populate data set. */
    private int _onPopulateDataSet;
    
    /** The on persist data set. */
    private int _onPersistDataSet;
    
    /** The linked source name. */
    private String _source;
    
    /**
     * Instantiates a new source.
     */
    public Source()
    {
        super();
        
        _name = null;
        _objectName = null;
        _sql = null;
        _using = null;
        _variables = new ListHashMap<String, Variable>();
        _tasks = null;
        _postTasks = null;
        _inlineTasks = null;
        _beforeEtlTasks = null;
        _dataSet = null;
        _noConnection = false;
        _connectionName = null;
        _connection = null;
        _independent = false;
        _empty = false;
        _encode = true;
        _enabled = true;
        _parallel = false;
        _mandatory = false;
        _dataWriterClassName = null;
        _dataReaderClassName = null;
        _dataWriterParams = null;
        _dataReaderParams = null;
        _usageCounter = 0;
        
        _keyName = null;
        _keyField = null;
        
        _driverClassName = null;
        
        _onPersistDataSet = Scenario.ON_ACTION_PARENT;
        _onPopulateDataSet = Scenario.ON_ACTION_PARENT;
        
        _source = null;
    }
    
    /**
     * Decrements usage counter.
     */
    public synchronized void decUsageCounter()
    {
        _usageCounter--;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#getBeforeEtlTasks()
     */
    public ListHashMap<String, Task> getBeforeEtlTasks()
    {
        return _beforeEtlTasks;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#getConnection()
     */
    public Connection getConnection()
    {
        return _connection;
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
     * Gets the data reader class name. Data reader must implement DataSetConnector interface.
     *
     * @return the data reader class name
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
     * Gets the data writer class name. Data writer must implement DataSetConnector interface.
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
                : EtlConfig.SOURCE_CONNECTION_NAME;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.Block#getDriverClassName()
     */
    public String getDriverClassName()
    {
        return _driverClassName;
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
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.common.OnException#getKeyField()
     */
    @Override
    public String getKeyFields()
    {
        return _keyField;
    }
    
    /**
     * Gets the key name.
     *
     * @return the key name
     */
    public String getKeyName()
    {
        return _keyName;
    }
    
    /**
     * Gets the name of the linked source.
     * 
     * @return the name of the linked source
     */
    public String getLinkedSourceName()
    {
        return _source;
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
     * Gets the name of the object. Can be a table name. It used by registered code generator to create database
     * specific load code for the destination.
     * 
     * @return the object name
     */
    public String getObjectName()
    {
        return _objectName;
    }
    
    /**
     * Gets the action on "persist data set" event. 
     *
     * @return the action on "persist data set" event
     */
    public int getOnPersistDataSet()
    {
        return _onPersistDataSet;
    }
    
    /**
     * Gets the action on "persist data set" from then given action name.
     *
     * @param action the action
     * @return the action on "persist data set" event
     */
    public int getOnPersistDataSet(String action)
    {
        if (action == null)
            return Scenario.ON_ACTION_SKIP;
        
        Integer value = Scenario.ON_ACTION.get(action.toLowerCase());
        
        if (value == null)
            return Scenario.ON_ACTION_SAVE;
        
        return value.intValue();
    }
    
    /**
     * Gets the action on "populate data set" event.
     *
     * @return the action on "populate data set" event
     */
    public int getOnPopulateDataSet()
    {
        return _onPopulateDataSet;
    }
    
    /**
     * Gets the action on "populate data set" event from the given action name.
     *
     * @param action the action
     * @return the action on "populate data set" event
     */
    public int getOnPopulateDataSet(String action)
    {
        if (action == null)
            return Scenario.ON_ACTION_SKIP;
        
        Integer value = Scenario.ON_ACTION.get(action.toLowerCase());
        
        if (value == null)
            return Scenario.ON_ACTION_SKIP;
        
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
     * Gets the sql.
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
     * Gets the usage counter.
     *
     * @return the usage counter
     */
    public int getUsageCounter()
    {
        return _usageCounter;
    }
    
    /**
     * Gets the "using". "Using" is comma delimited string of bind variables.
     *
     * @return the "using"
     */
    public String getUsing()
    {
        return _using;
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
     * Increments usage counter.
     */
    public synchronized void incUsageCounter()
    {
        _usageCounter++;
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
    
    /**
     * Checks if source is independent. Independent source is not linked to any destination.
     *
     * @return true, if is independent
     */
    public Boolean isIndependent()
    {
        return _independent;
    }
    
    /**
     * Checks if source is mandatory. Mandatory source must be extracted before load, otherwise extract and load can be combined to implement data streaming. 
     *
     * @return true, if is mandatory
     */
    public boolean isMandatory()
    {
        return _mandatory;
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
     * Checks if source is a stub. If source doesn't have sql or any tasks and there is no linked data reader it is a "stub" source. 
     * If multiple sources are configured to extract in parallel the "stub" can be used as a "gate" - all parallel tasks must be finished 
     * in order to pass through the gate.
     *
     * @return true, if it is a stub
     */
    public boolean isStub()
    {
        return Utils.isNothing(getSql())
                && Utils.isNothing(getDataReaderClassName())
                && (getTasks() == null || getTasks().size() == 0);
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
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.Block#setBeforeEtlTasks(com.toolsverse
     * .util.ListHashMap)
     */
    public void setBeforeEtlTasks(ListHashMap<String, Task> value)
    {
        _beforeEtlTasks = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.Block#setConnection(java.sql.Connection)
     */
    public void setConnection(Connection value)
    {
        _connection = value;
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
     * Sets the data reader class name. The data reader must implement DataSetConnector interface.
     *
     * @param value the new data reader class name
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
    }
    
    /**
     * Sets the data writer class name. The data writer must implement DataSetConnector interface.
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
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.Block#setDriverClassName(java.lang.String)
     */
    public void setDriverClassName(String value)
    {
        _driverClassName = value;
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
     * Sets the "is independent" flag.
     *
     * @param value the new value for "is independent" flag
     */
    public void setIsIndependent(Boolean value)
    {
        _independent = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.common.OnException#setKeyField(java.lang.String)
     */
    @Override
    public void setKeyFields(String value)
    {
        _keyField = value;
    }
    
    /**
     * Sets the key name.
     *
     * @param value the new key name
     */
    public void setKeyName(String value)
    {
        _keyName = value;
    }
    
    /**
     * Sets the name of the linked source.
     * 
     * @param value the new name of the linked source.
     */
    public void setLinkedSourceName(String value)
    {
        _source = value;
    }
    
    /**
     * Sets the "mandatory" flag.
     *
     * @param value the new value for the "mandatory" flag
     */
    public void setMandatory(boolean value)
    {
        _mandatory = value;
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
     * Sets the "no connection" flag.
     *
     * @param value the new value for the "no connection" flag
     */
    public void setNoConnection(boolean value)
    {
        _noConnection = value;
    }
    
    /**
     * Sets the name of the object.
     * 
     * <p>
     * Example of the <code>objectname</code> at the source level in the
     * scenario file: <blockquote>
     * <dt>{@code <source>}</dt>
     * <dd>{@code <name>test</name>}</dt>
     * </br>
     * <dd>{@code <objectname>ABC.CLIENT</objectname>}</dt>
     * </br>
     * <dt>{@code </source>}</dt>
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
     * Sets the action for "on persist data set" event.
     *
     * @param value the new action for the "on persist data set" event
     */
    public void setOnPersistDataSet(int value)
    {
        _onPersistDataSet = value;
    }
    
    /**
     * Sets the action for "on persist data set" event.
     *
     * @param action the new action for "on persist data set" event
     */
    public void setOnPersistDataSet(String action)
    {
        _onPersistDataSet = getOnPersistDataSet(action);
    }
    
    /**
     * Sets the action for "on populate data set" event.
     *
     * @param value the new action for the "on populate data set" event
     */
    public void setOnPopulateDataSet(int value)
    {
        _onPopulateDataSet = value;
    }
    
    /**
     * Sets the action for "on populate data set" event.
     *
     * @param action the new action for "on populate data set" event
     */
    public void setOnPopulateDataSet(String action)
    {
        _onPopulateDataSet = getOnPopulateDataSet(action);
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
     * Sets the sql.
     *
     * @param value the new sql
     */
    public void setSql(String value)
    {
        _sql = value;
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
     * Sets the "using". "Using" is a comma delimited string with the names of the bind variables.  
     *
     * @param value the new "using"
     */
    public void setUsing(String value)
    {
        _using = value;
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
}
