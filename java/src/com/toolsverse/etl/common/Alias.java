/*
 * Alias.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

import java.io.Serializable;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.connector.sql.SqlConnector;
import com.toolsverse.util.Utils;

/**
 * This class includes all parameters needed to create a database connection,
 * such as url, user name, password, etc. It can also be used to access
 * file-based sources, such as xml files, text files, etc.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public final class Alias implements ConnectionParamsProvider<Alias>,
        ConnectionParams, Serializable
{
    
    /** The name. */
    private String _name;
    
    /** The unique id. */
    private String _uniqueId;
    
    /** The jdbc driver class. */
    private String _jdbcDriverClass;
    
    /** The url. */
    private String _url;
    
    /** The user id. */
    private String _userId;
    
    /** The password. */
    private String _password;
    
    /** The parameters. */
    private String _params;
    
    /** The init sql. */
    private String _initSql;
    
    /** The local commit. */
    private boolean _localCommit;
    
    /** The connector class name. */
    private String _connectorClassName;
    
    /**
     * Instantiates a new Alias.
     */
    public Alias()
    {
        _name = null;
        _uniqueId = null;
        _jdbcDriverClass = null;
        _url = null;
        _userId = null;
        _password = null;
        _params = null;
        _initSql = null;
        _localCommit = true;
        _connectorClassName = null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.common.ConnectionParams#copy(java.lang.String)
     */
    public ConnectionParams copy(String id)
    {
        Alias alias = new Alias();
        
        alias._name = id;
        
        alias._uniqueId = id;
        
        alias._jdbcDriverClass = _jdbcDriverClass;
        
        alias._url = _url;
        
        alias._userId = _userId;
        
        alias._password = _password;
        
        alias._params = _params;
        
        alias._initSql = _initSql;
        
        alias._localCommit = _localCommit;
        
        alias._connectorClassName = _connectorClassName;
        
        return alias;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Alias other = (Alias)obj;
        if (_name == null)
        {
            if (other._name != null)
                return false;
        }
        else if (!_name.equals(other._name))
            return false;
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.common.ConnectionParamsProvider#getConnectionParams()
     */
    public Alias getConnectionParams()
    {
        return this;
    }
    
    /**
     * Gets the connector class name. The connector is class to populate and
     * persist data. For example SqlConnector uses sql to read data from the
     * database, TextConnector reads text files, etc. Can be null. Default -
     * SqlConnector.
     * 
     * @return the connector class name
     */
    public String getConnectorClassName()
    {
        return _connectorClassName;
    }
    
    /**
     * Gets the sql which will be executed right after JDBC connection is
     * established. Not used in "file mode".
     * <p>
     * <b>Example:</b></br>
     * 
     * <pre>
     * init();
     * insert into test values(1);
     * </pre>
     * 
     * @return sql
     */
    public String getInitSql()
    {
        return _initSql;
    }
    
    /**
     * Gets the name of the driver for the JDBC database connection.
     * <p>
     * <b>Example:</b>
     * <p>
     * <code>oracle.jdbc.driver.OracleDriver</code>
     * 
     * @return the name of the driver
     */
    public String getJdbcDriverClass()
    {
        return _jdbcDriverClass;
    }
    
    /**
     * Gets name of the alias.
     * <p>
     * <b>Example:</b>
     * <p>
     * <code>informix etl</code>
     * 
     * @return the name of the alias
     */
    public String getName()
    {
        return _name;
    }
    
    /**
     * Gets the additional parameters. The expected format:
     * name=value;name=value;etc
     * 
     * <p>
     * <b>Example:</b>
     * <p>
     * <code>SERVER=ol_svr_custom;DB=etl</code>
     * 
     * @return additional parameters
     */
    public String getParams()
    {
        return _params;
    }
    
    /**
     * Gets the password for the JDBC database connection.
     * <p>
     * NOTE: password is not encrypted
     * 
     * @return password
     */
    public String getPassword()
    {
        return _password;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.common.ConnectionParams#getUniqueId()
     */
    public String getUniqueId()
    {
        return _uniqueId != null ? _uniqueId : _name;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.common.ConnectionParams#getUniqueProperty()
     */
    public String getUniqueProperty()
    {
        return getJdbcDriverClass();
    }
    
    /**
     * Gets the url. It is possible to use system variables {app.home} and
     * {app.data}.
     * <p>
     * <b>Example:</b>
     * <p>
     * <code>jdbc:oracle:thin:@dev:1521:txn</code>
     * <p>
     * <code>{app.data}/test.xml</code>
     * 
     * @return the url
     */
    public String getUrl()
    {
        return SystemConfig.instance().getPathUsingAppFolders(_url);
    }
    
    /**
     * Gets the user id for the JDBC database connection.
     * 
     * @return user id
     */
    public String getUserId()
    {
        return _userId;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_name == null) ? 0 : _name.hashCode());
        return result;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.common.ConnectionParams#isDbConnection()
     */
    public boolean isDbConnection()
    {
        return Utils.isNothing(_connectorClassName)
                || SqlConnector.class.getName().equals(_connectorClassName);
    }
    
    /**
     * Checks if local commit field is set for the connection. Local commit
     * means that successful transaction associated with the connection will be
     * automatically committed by the etl framework when etl process is
     * finished. Otherwise commit is delegated to the calling code, for example
     * ejb container.
     * 
     * @return <code>true</code>, if local commit field set for the connection
     */
    public boolean isLocalCommit()
    {
        return _localCommit;
    }
    
    /**
     * Sets the connector class name.
     * 
     * @param value
     *            the new connector class name
     */
    public void setConnectorClassName(String value)
    {
        _connectorClassName = value;
    }
    
    /**
     * Sets the sql which will be executed right after JDBC connection is
     * established. Semicolons can be used to split string on multiple lines.
     * Each of them will be executed separately.
     * <p>
     * <b>Example:</b></br>
     * 
     * <pre>
     * call init();
     * insert into test  values(1);
     * </pre>
     * 
     * @param value
     *            The new init sql
     */
    public void setInitSql(String value)
    {
        _initSql = value;
    }
    
    /**
     * Sets the class name of the driver for the JDBC database connection.
     * <p>
     * <b>Example:</b>
     * <p>
     * <code>oracle.jdbc.driver.OracleDriver</code>
     * 
     * @param value
     *            The new class name of the driver
     */
    public void setJdbcDriverClass(String value)
    {
        _jdbcDriverClass = value;
    }
    
    /**
     * Sets the value for local commit field. Local commit means that successful
     * transaction associated with the connection will be automatically
     * committed by the etl framework when etl process is finished. Otherwise
     * commit is delegated to the calling code, for example ejb container.
     * 
     * @param value
     *            The new local commit value
     */
    public void setLocalCommit(boolean value)
    {
        _localCommit = value;
    }
    
    /**
     * Sets the name of the alias.
     * <p>
     * <b>Example:</b><code>informix etl</code>
     * 
     * @param value
     *            The new name of the alias
     */
    public void setName(String value)
    {
        _name = value;
    }
    
    /**
     * Sets the additional parameters for the connection.
     * <p>
     * <b>Example:</b><code>SERVER=ol_svr_custom;DB=etl</code>
     * 
     * @param value
     *            The new additional parameters
     */
    public void setParams(String value)
    {
        _params = value;
    }
    
    /**
     * Sets the password of the JDBC database connection.
     * <p>
     * NOTE: password is not encrypted
     * 
     * @param value
     *            The new password
     */
    public void setPassword(String value)
    {
        _password = value;
    }
    
    /**
     * Sets the unique id.
     * 
     * @param value
     *            the new unique id
     */
    public void setUniqueId(String value)
    {
        _uniqueId = value;
    }
    
    /**
     * Sets the url for the JDBC database connection.
     * <p>
     * <b>Example:</b>
     * <p>
     * <code>jdbc:oracle:thin:@dev:1521:txn</code>
     * 
     * @param value
     *            The new url
     */
    public void setUrl(String value)
    {
        _url = value;
    }
    
    /**
     * Sets the user id of the JDBC database connection.
     * 
     * @param value
     *            The new user id
     */
    public void setUserId(String value)
    {
        _userId = value;
    }
    
}
