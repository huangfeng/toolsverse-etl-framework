/*
 * Task.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.OnException;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.Utils;

/**
 * This is a class which contains all data related to the etl task. The etl building blocks, such as Source and Destination can have one or multiple tasks attached. 
 * The tasks can be executed before, after, during, etc extract and load. For example there can be a task configured to execute after extract which 
 * copies data files from the local machine to the remove ftp server. The actual tasks must implement <code>OnTask</code> interface.  
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class Task extends OnException implements ObjectStorage
{
    /** The AS_CONFIGURED - use default settings for the task. */
    public static final int AS_CONFIGURED = 0;
    
    /** The PRE task - executed before extract or load. */
    public static final int PRE = 2;
    
    /** The POST task - executed after extract or load. */
    public static final int POST = 4;
    
    /** The INLINE task - executed for each row of the data set druring extract. */
    public static final int INLINE = 8;
    
    /** The BEFORE_ETL task - executed before etl process has started. */
    public static final int BEFORE_ETL = 16;
    
    /** The PRE task code. */
    public static final String PRE_STR = "pre";
    
    /** The POST task code. */
    public static final String POST_STR = "after";
    
    /** The INLINE task code. */
    public static final String INLINE_STR = "inline";
    
    /** The BEFORE_ETL task code. */
    public static final String BEFORE_ETL_STR = "before_etl";
    
    /** The SCOPE. */
    public static final Map<String, Integer> SCOPE = new HashMap<String, Integer>();
    static
    {
        SCOPE.put(PRE_STR, new Integer(PRE));
        SCOPE.put(POST_STR, new Integer(POST));
        SCOPE.put(INLINE_STR, new Integer(INLINE));
        SCOPE.put(BEFORE_ETL_STR, new Integer(BEFORE_ETL));
    }
    
    /** The name. */
    private String _name;
    
    /** The class name. */
    private String _className;
    
    /** The connection name. */
    private String _connectionName;
    
    /** The source. */
    private Source _source;
    
    /** The no connection flag. */
    private boolean _noConnection;
    
    /** The connection. */
    transient private Connection _connection;
    
    /** The data set. */
    private DataSet _dataSet;
    
    /** The driver class name. */
    private String _driverClassName;
    
    /** The driver. */
    private Driver _driver;
    
    /** The table name. */
    private String _tableName;
    
    /** The code. */
    private String _code;
    
    /** The using. */
    private String _using;
    
    /** The variables. */
    private ListHashMap<String, Variable> _variables;
    
    /** The on action. */
    private OnTask _onTask;
    
    /** The _scope. */
    private int _scope;
    
    /** The _commit. */
    private boolean _commit;
    
    /** The storage. */
    transient private Map<String, Object> _storage;
    
    /** The scenario */
    transient private Scenario _scenario;
    
    /**
     * Instantiates a new task.
     */
    public Task()
    {
        super();
        
        _name = null;
        _className = null;
        _noConnection = false;
        _connectionName = null;
        _source = null;
        _connection = null;
        _dataSet = null;
        _driverClassName = null;
        _driver = null;
        _tableName = null;
        _code = null;
        _using = null;
        _variables = new ListHashMap<String, Variable>();
        _onTask = null;
        _scope = AS_CONFIGURED;
        _commit = true;
        _storage = new HashMap<String, Object>();
        _scenario = null;
    }
    
    /**
     * Adds the scope. The same task can be executed at the various stages (pre, port, etc).   
     *
     * @param value the new scope. Possible values: {@link Task#PRE}, {@link Task#POST}, {@link Task#INLINE}, {@link Task#BEFORE_ETL} 
     */
    public void addScope(int value)
    {
        _scope = _scope | value;
    }
    
    /**
     * Adds the variable.
     *
     * @param var the variable
     */
    public void addVariable(Variable var)
    {
        if (_variables == null)
            _variables = new ListHashMap<String, Variable>();
        
        _variables.put(var.getName(), var);
    }
    
    /**
     * Adds the variables.
     *
     * @param value the variables
     */
    public void addVariables(ListHashMap<String, Variable> value)
    {
        if (value == null)
            _variables = value;
        else
        {
            if (_variables == null)
                _variables = new ListHashMap<String, Variable>();
            
            for (int i = 0; i < value.size(); i++)
            {
                Variable variable = value.get(i);
                
                _variables.set(variable.getName(), variable, true);
            }
        }
    }
    
    /**
     * Checks if commit need to be executed when done.
     *
     * @return true, if successful
     */
    public boolean commitWhenDone()
    {
        return _commit;
    }
    
    /**
     * Gets the task class name. The task must implement <code>OnTask</code> interface. 
     *
     * @return the task class name
     */
    public String getClassName()
    {
        return _className;
    }
    
    /**
     * Gets the code. Typically sql, but can be other languages as well.
     *
     * @return the code
     */
    public String getCode()
    {
        return _code;
    }
    
    /**
     * Gets the connection.
     *
     * @return the connection
     */
    public Connection getConnection()
    {
        return _connection;
    }
    
    /**
     * Gets the connection name.
     *
     * @return the connection name
     */
    public String getConnectionName()
    {
        return _connectionName;
    }
    
    /**
     * Gets the data set.
     *
     * @return the data set
     */
    public DataSet getDataSet()
    {
        return _dataSet;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.common.ConditionalExecution#getDefaultConnectionName()
     */
    @Override
    public String getDefaultConnectionName()
    {
        return _connectionName;
    }
    
    /**
     * Gets the driver.
     *
     * @return the driver
     */
    public Driver getDriver()
    {
        return _driver;
    }
    
    /**
     * Gets the driver class name.
     *
     * @return the driver class name
     */
    public String getDriverClassName()
    {
        return _driverClassName;
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
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.common.OnException#getOnExceptionActions(java.lang
     * .String)
     */
    @Override
    public int getOnExceptionActions(String action)
    {
        if (action == null)
            return ON_EXCEPTION_RAISE;
        
        Integer value = ON_EXCEPTION.get(action.toLowerCase());
        
        if (value == null)
            return ON_EXCEPTION_RAISE;
        
        return value.intValue();
    }
    
    /**
     * Gets the instance of the class implementing OnTask interface. The class name defined be <code>getClassName()</code>. 
     *
     * @return the OnTask instance 
     */
    public OnTask getOnTask()
    {
        return _onTask;
    }
    
    /**
     * Gets the scenario.
     * 
     * @return the scenario
     */
    public Scenario getScenario()
    {
        return _scenario;
    }
    
    /**
     * Gets the scope. The same task can be executed at the various stages: {@link Task#PRE}, {@link Task#POST}, {@link Task#INLINE}, {@link Task#BEFORE_ETL}
     *
     * @return the scope
     */
    public int getScope()
    {
        return _scope;
    }
    
    /**
     * Gets the source.
     *
     * @return the source
     */
    public Source getSource()
    {
        return _source;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#getString(java.lang.String)
     */
    public String getString(String key)
    {
        Object value = _storage.get(key);
        
        return value != null ? value.toString() : null;
    }
    
    /**
     * Gets the table name.
     *
     * @return the table name
     */
    public String getTableName()
    {
        return _tableName;
    }
    
    /**
     * Gets the "using". "Using" is a comma delimited list of bind variables.  
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
     * @see com.toolsverse.util.ObjectStorage#getValue(java.lang.String)
     */
    public Object getValue(String key)
    {
        return _storage.get(key);
    }
    
    /**
     * Gets the variable by name.
     *
     * @param name the name
     * @return the variable
     */
    public Variable getVariable(String name)
    {
        if (_variables == null)
            return null;
        
        return _variables.get(name);
    }
    
    /**
     * Gets the variables.
     *
     * @return the variables
     */
    public ListHashMap<String, Variable> getVariables()
    {
        return _variables;
    }
    
    /**
     * Checks if scope is set.
     *
     * @param value possible values: Task.PRE, Task.POST, Task.INLINE, Task.BEFORE_ETL 
     * @return true, if scope is set
     */
    public boolean isScopeSet(int value)
    {
        return ((_scope & value) == value);
    }
    
    /**
     * "No connection" flag.
     *
     * @return true, if true no connection is associated with the task
     */
    public boolean noConnection()
    {
        return _noConnection;
    }
    
    /**
     * Parses the scope.
     * 
     * <br>Example: pre|post  
     *
     * @param scope the "|" delimited string of possible scope codes
     * @return the scope
     */
    public int parseScope(String scope)
    {
        if (Utils.isNothing(scope))
        {
            _scope = AS_CONFIGURED;
            return _scope;
        }
        
        String[] scopes = scope.split("\\|", -1);
        
        for (int i = 0; i < scopes.length; i++)
        {
            Integer value = SCOPE.get(scopes[i].toLowerCase().trim());
            
            if (value != null)
                addScope(value.intValue());
        }
        
        return _scope;
    }
    
    /**
     * Sets the class name. The class must implement <code>OnTask</code> interface.
     *
     * @param value the new class name
     */
    public void setClassName(String value)
    {
        _className = value;
    }
    
    /**
     * Sets the code. Typically sql but other languages, for example JavaScript can be used as well. 
     *
     * @param value the new code
     */
    public void setCode(String value)
    {
        _code = value;
    }
    
    /**
     * Sets the "commit when done" flag.
     *
     * @param value the new value for the "commit when done" flag
     */
    public void setCommitWhenDone(boolean value)
    {
        _commit = value;
    }
    
    /**
     * Sets the connection.
     *
     * @param value the new connection
     */
    public void setConnection(Connection value)
    {
        _connection = value;
    }
    
    /**
     * Sets the connection name.
     *
     * @param value the new connection name
     */
    public void setConnectionName(String value)
    {
        _connectionName = value;
    }
    
    /**
     * Sets the data set.
     *
     * @param value the new data set
     */
    public void setDataSet(DataSet value)
    {
        _dataSet = value;
    }
    
    /**
     * Sets the driver.
     *
     * @param value the new driver
     */
    public void setDriver(Driver value)
    {
        _driver = value;
    }
    
    /**
     * Sets the driver class name.
     *
     * @param value the new driver class name
     */
    public void setDriverClassName(String value)
    {
        _driverClassName = value;
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
     * @param value the new value for the "no connection" flag. If true there is no jdbc connection associated with the task. 
     */
    public void setNoConnection(boolean value)
    {
        _noConnection = value;
    }
    
    /**
     * Sets the instance of the <code>OnTask</code> interface.
     *
     * @param value the new instance of the <code>OnTask</code> interface
     */
    public void setOnTask(OnTask value)
    {
        _onTask = value;
    }
    
    /**
     * Sets the scenario.
     * 
     * @param value the scenario
     */
    public void setScenario(Scenario value)
    {
        _scenario = value;
    }
    
    /**
     * Sets the source.
     *
     * @param value the new source
     */
    public void setSource(Source value)
    {
        _source = value;
    }
    
    /**
     * Sets the table name.
     *
     * @param value the new table name
     */
    public void setTableName(String value)
    {
        _tableName = value;
    }
    
    /**
     * Sets the "using". "Using" is a comma delimited list of bind variables. 
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
     * @see com.toolsverse.util.ObjectStorage#setValue(java.lang.String,
     * java.lang.Object)
     */
    public void setValue(String key, Object value)
    {
        if (key == null)
            return;
        
        _storage.put(key, value);
    }
    
    /**
     * Sets the variables.
     *
     * @param value the variables
     */
    public void setVariables(ListHashMap<String, Variable> value)
    {
        _variables = value;
    }
    
}
