/*
 * SqlRequest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.ConnectionParamsProvider;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.parser.SqlParser;
import com.toolsverse.etl.sql.connection.SessionConnectionProvider;
import com.toolsverse.util.ObjectStorage;

/**
 * SqlRequest is a data structure passed as a parameter to the SqlService methods. Contains memmebrs such as sql, parameters, connectio provider, etc
 * needed to retrieve objects from the database.
 * 
 * @see com.toolsverse.etl.sql.service.SqlService
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SqlRequest implements ObjectStorage, Serializable
{
    
    /** The connection provider. */
    private SessionConnectionProvider<Alias> _provider;
    
    /** The connection params provider. */
    private ConnectionParamsProvider<Alias> _paramsProvider;
    
    /** The sql parser. */
    private SqlParser _parser;
    
    /** The driver. */
    private Driver _driver;
    
    /** The properties. */
    private Map<String, Object> _properties;
    
    /** The sql. */
    private String _sql;
    
    /** The "has params" flag. */
    private boolean _hasParams;
    
    /** The "commit when done" flag. */
    private boolean _commit;
    
    /** The "close connection when done" flag. */
    private boolean _close;
    
    /** The maximum number of rows rows. */
    private int _maxRows;
    
    /** The storage. */
    private Map<String, Object> _storage;
    
    /**
     * Instantiates a new sql request.
     */
    public SqlRequest()
    {
        _storage = new HashMap<String, Object>();
        
        _provider = null;
        
        _paramsProvider = null;
        
        _parser = null;
        
        _driver = null;
        
        _properties = null;
        
        _sql = null;
        
        _hasParams = false;
        
        _commit = false;
        
        _close = false;
        
        _maxRows = -1;
    }
    
    /**
     * Instantiates a new sql request.
     *
     * @param provider the connection provider
     */
    public SqlRequest(SessionConnectionProvider<Alias> provider)
    {
        this(provider, null, null, null, null, null, false, false, false, -1);
    }
    
    /**
     * Instantiates a new sql request.
     *
     * @param provider the connection provider
     * @param paramsProvider the connection params provider
     * @param parser the sql parser
     * @param driver the driver
     * @param properties the properties
     * @param sql the sql
     * @param hasParams the "has params" flag
     */
    public SqlRequest(SessionConnectionProvider<Alias> provider,
            ConnectionParamsProvider<Alias> paramsProvider, SqlParser parser,
            Driver driver, Map<String, Object> properties, String sql,
            boolean hasParams)
    {
        this(provider, paramsProvider, parser, driver, properties, sql,
                hasParams, false, false);
    }
    
    /**
     * Instantiates a new sql request.
     *
     * @param provider the connection provider
     * @param paramsProvider the connection params provider
     * @param parser the sql parser
     * @param driver the driver
     * @param properties the properties
     * @param sql the sql
     * @param hasParams the "has params" flag
     * @param commit the "commit when done" flag
     * @param close the "close when done" flag
     */
    public SqlRequest(SessionConnectionProvider<Alias> provider,
            ConnectionParamsProvider<Alias> paramsProvider, SqlParser parser,
            Driver driver, Map<String, Object> properties, String sql,
            boolean hasParams, boolean commit, boolean close)
    {
        this(provider, paramsProvider, parser, driver, properties, sql,
                hasParams, commit, close, -1);
    }
    
    /**
     * Instantiates a new sql request.
     *
     * @param provider the connection provider
     * @param paramsProvider the connection params provider
     * @param parser the sql parser
     * @param driver the driver
     * @param properties the properties
     * @param sql the sql
     * @param hasParams the "has params" flag
     * @param commit the "commit when done" flag
     * @param close the "close when done" flag
     * @param maxRows the maximum number of rows
     */
    public SqlRequest(SessionConnectionProvider<Alias> provider,
            ConnectionParamsProvider<Alias> paramsProvider, SqlParser parser,
            Driver driver, Map<String, Object> properties, String sql,
            boolean hasParams, boolean commit, boolean close, int maxRows)
    {
        this();
        
        _provider = provider;
        _paramsProvider = paramsProvider;
        _parser = parser;
        _driver = driver;
        _properties = properties;
        _sql = sql;
        _hasParams = hasParams;
        _commit = commit;
        _close = close;
        _maxRows = maxRows;
    }
    
    /**
     * Gets the connection params provider.
     *
     * @return the connection params provider
     */
    public ConnectionParamsProvider<Alias> getConnectionParamsProvider()
    {
        return _paramsProvider;
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
     * Gets the maximum number of rows.
     *
     * @return the maximum number of rows
     */
    public int getMaxRows()
    {
        return _maxRows;
    }
    
    /**
     * Gets the sql parser.
     * 
     * @return the sql parser
     */
    public SqlParser getParser()
    {
        return _parser;
    }
    
    /**
     * Gets the properties.
     * 
     * @return the properties
     */
    public Map<String, Object> getProperties()
    {
        return _properties;
    }
    
    /**
     * Gets the connection provider.
     * 
     * @return the connection provider
     */
    public SessionConnectionProvider<Alias> getProvider()
    {
        return _provider;
    }
    
    /**
     * Gets the sql to execute.
     * 
     * @return the sql to execute
     */
    public String getSql()
    {
        return _sql;
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
     * Checks if "has params" flag is set.
     * 
     * @return true, if successful
     */
    public boolean hasParams()
    {
        return _hasParams;
    }
    
    /**
     * Checks if "close when done" flag is set.
     * 
     * @return true, if "close when done" flag is set
     */
    public boolean isClose()
    {
        return _close;
    }
    
    /**
     * Checks if "commit when done" flag is set.
     * 
     * @return true, if "commit when done" flag is set
     */
    public boolean isCommit()
    {
        return _commit;
    }
    
    /**
     * Sets the connection params provider.
     *
     * @param paramsProvider the new connection params provider
     */
    public void setConnectionParamsProvider(
            ConnectionParamsProvider<Alias> paramsProvider)
    {
        _paramsProvider = paramsProvider;
    }
    
    /**
     * Sets the driver.
     * 
     * @param driver
     *            the new driver
     */
    public void setDriver(Driver driver)
    {
        _driver = driver;
    }
    
    /**
     * Sets the "has params" flag.
     * 
     * @param hashParams
     *            the new value for the "has params" flag
     */
    public void setHasParams(boolean hashParams)
    {
        _hasParams = hashParams;
    }
    
    /**
     * Sets the sql parser.
     * 
     * @param parser
     *            the new sql parser
     */
    public void setParser(SqlParser parser)
    {
        _parser = parser;
    }
    
    /**
     * Sets the properties.
     * 
     * @param properties
     *            the new properties
     */
    public void setProperties(LinkedHashMap<String, Object> properties)
    {
        _properties = properties;
    }
    
    /**
     * Sets the connection provider.
     * 
     * @param provider
     *            the new connection provider
     */
    public void setProvider(SessionConnectionProvider<Alias> provider)
    {
        _provider = provider;
    }
    
    /**
     * Sets the sql.
     * 
     * @param sql
     *            the new sql
     */
    public void setSql(String sql)
    {
        _sql = sql;
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
}
