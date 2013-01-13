/*
 * BaseMetadata.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.metadata;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.ConnectionParamsProvider;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.sql.connection.ConnectionFactory;
import com.toolsverse.ext.ExtUtils;
import com.toolsverse.ext.ExtensionModule;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * The base abstract implementation of the Metadata interface.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public abstract class BaseMetadata implements Metadata
{
    
    /** The connection params provider. */
    private ConnectionParamsProvider<Alias> _connectionParamsProvider;
    
    /** The connection factory. */
    private ConnectionFactory _connectionFactory;
    
    /** The driver. */
    private Driver _driver = null;
    
    /** The "has catalogs" flag. */
    private boolean _hasCatalogs = false;
    
    /** The "has schemas" flag. */
    private boolean _hasSchemas = false;
    
    /** The current database. */
    private String _currentDatabase = null;
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#asText(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String)
     */
    public String asText(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(ExtensionModule ext)
    {
        return ExtUtils.compareTo(this, ext);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getConfigFileName()
     */
    public String getConfigFileName()
    {
        return null;
    }
    
    /**
     * Gets the connection factory.
     *
     * @return the connection factory
     */
    public ConnectionFactory getConnectionFactory()
    {
        return _connectionFactory;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#getConnectionParamsProvider()
     */
    public ConnectionParamsProvider<Alias> getConnectionParamsProvider()
    {
        return _connectionParamsProvider;
    }
    
    /**
     * Gets the current database.
     *
     * @return the current database
     */
    public String getCurrentDatabase()
    {
        return _currentDatabase;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getDisplayName()
     */
    public String getDisplayName()
    {
        return getName();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#getDriver()
     */
    public Driver getDriver()
    {
        return _driver;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.Metadata#getFullObjectName(java.lang.String)
     */
    public String getFullObjectName(String pattern)
    {
        if (pattern == null)
            return null;
        
        return pattern.replaceAll(METADATA_DELIMITER, DB_DELIMITER);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getIconPath()
     */
    public String getIconPath()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getLocalUnitClassPath()
     */
    public String getLocalUnitClassPath()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.Metadata#getMetadataByType(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String)
     */
    @SuppressWarnings("unchecked")
    public DataSet getMetadataByType(String catalog, String schema,
            String pattern, String type)
        throws Exception
    {
        String name = getMetadataMethods().get(type);
        
        if (Utils.isNothing(name))
            name = DEFAULT_METHOD;
        
        Method invokeMethod = null;
        
        Class<String>[] parametertypes = new Class[] {String.class,
                String.class, String.class, String.class};
        
        invokeMethod = getClass().getMethod(name, parametertypes);
        
        try
        {
            return (DataSet)invokeMethod.invoke(this, new Object[] {catalog,
                    schema, pattern, type});
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this, Resource.ERROR_GENERAL.getValue(),
                    ex);
            
            return null;
        }
    }
    
    /**
     * Gets the metadata methods.
     *
     * @return the metadata methods
     */
    public abstract Map<String, String> getMetadataMethods();
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.Metadata#getMetadataTypeByParentType(java
     * .lang.String)
     */
    public String getMetadataTypeByParentType(String parentType)
    {
        return getTypesByParent().get(parentType);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.Metadata#getMetadataTypes(java.lang.String)
     */
    @SuppressWarnings("unchecked")
    public List<Object> getMetadataTypes(String parentType)
        throws Exception
    {
        String[] names = parentType.split(";", -1);
        
        if (names == null || names.length == 0)
            return null;
        
        Method invokeMethod = null;
        
        List<Object> list = new ArrayList<Object>();
        
        for (int i = 0; i < names.length; i++)
        {
            String name = getTypeMethods().get(names[i]);
            
            if (name == null)
                continue;
            
            invokeMethod = getClass().getMethod(name, (Class[])null);
            
            list.addAll((List<Object>)invokeMethod.invoke(this, (Object[])null));
        }
        
        return list;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#getObjectName(java.lang.String)
     */
    public String getObjectName(String pattern)
    {
        if (pattern == null)
            return null;
        
        String[] values = pattern.split(METADATA_DELIMITER);
        
        return values[values.length - 1];
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.Metadata#getObjectOwnerName(java.lang.String)
     */
    public String getObjectOwnerName(String pattern)
    {
        if (pattern == null)
            return null;
        
        String[] values = pattern.split(METADATA_DELIMITER);
        
        if (values.length < 2)
            return pattern;
        
        String value = "";
        
        for (int i = 0; i < values.length - 1; i++)
        {
            if (i == 0)
                value = values[i];
            else
                value = value + DB_DELIMITER + values[i];
        }
        
        return value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getType()
     */
    public String getType()
    {
        return "Metadata Driver";
    }
    
    /**
     * Gets the types by parent.
     *
     * @return the types by parent
     */
    public abstract Map<String, String> getTypesByParent();
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getXmlConfigFileName()
     */
    public String getXmlConfigFileName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#hasCatalogs()
     */
    public boolean hasCatalogs()
    {
        return _hasCatalogs;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.Metadata#hasMetadataTypes(java.lang.String)
     */
    public boolean hasMetadataTypes(String type)
    {
        return getTypeMethods().containsKey(type);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#hasSchemas()
     */
    public boolean hasSchemas()
    {
        return _hasSchemas;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.Metadata#init(com.toolsverse.etl.sql.connection
     * .ConnectionFactory, com.toolsverse.etl.common.ConnectionParamsProvider,
     * com.toolsverse.etl.driver.Driver)
     */
    public void init(ConnectionFactory connectionFactory,
            ConnectionParamsProvider<Alias> connectionParamsProvider,
            Driver driver)
        throws Exception
    {
        _connectionParamsProvider = connectionParamsProvider;
        
        _connectionFactory = connectionFactory;
        
        _driver = driver;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.Metadata#isDatabaseCurrent(java.lang.String)
     */
    public boolean isDatabaseCurrent(String compareTo)
    {
        return compareTo != null
                && _currentDatabase != null
                && _currentDatabase.replaceAll("\\\\", "/").equalsIgnoreCase(
                        compareTo.replaceAll("\\\\", "/"));
    }
    
    /**
     * Sets the current database.
     *
     * @param value the new current database
     */
    public void setCurrentDatabase(String value)
    {
        _currentDatabase = value;
    }
    
    /**
     * Sets the value for the flag "has catalogs".
     *
     * @param value the new value for the flag "has catalogs"
     */
    public void setHasCatalogs(boolean value)
    {
        _hasCatalogs = value;
    }
    
    /**
     * Sets the value for the flag "has schemas".
     *
     * @param value the new value for the flag "has schemas".
     */
    public void setHasSchemas(boolean value)
    {
        _hasSchemas = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.Metadata#supportsAsText(java.lang.String)
     */
    public boolean supportsAsText(String type)
    {
        return false;
    }
    
}