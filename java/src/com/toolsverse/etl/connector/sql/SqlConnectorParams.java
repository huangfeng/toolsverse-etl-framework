/*
 * SqlConnectorParams.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.sql;

import java.io.Writer;
import java.sql.ResultSet;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;

import com.toolsverse.cache.CacheProvider;
import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.connector.DataSetConnectorParams;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.Utils;

/**
 * The {@link com.toolsverse.etl.connector.DataSetConnectorParams} used by {@link SqlConnector}.
 * 
 * @see com.toolsverse.etl.connector.sql.SqlConnector 
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class SqlConnectorParams extends DataSetConnectorParams
{
    
    /** The PREFIX. */
    public static final String PREFIX = "sql";
    
    /** The TABLE NAME property. */
    public static final String TABLE_NAME_PROP = "tablename";
    
    /** The result set. */
    private ResultSet _rs;
    
    /** The file name. */
    private String _fileName;
    
    /** The fields mapping. */
    private ListHashMap<String, String> _fieldsMapping;
    
    /** The unique flag. */
    private boolean _unique;
    
    /** The "check key field" flag. */
    private boolean _checkKeyField;
    
    /** The field name to filter by. */
    private String _filterByField;
    
    /** The value of the field to filter by. */
    private String _filterByFieldValue;
    
    /** The "file name required" flag. */
    private boolean _fileNameRequired;
    
    /** The real file name. */
    private String _realFileName;
    
    /** The table name. */
    private String _tableName;
    
    /** The writer. */
    private Writer _output;
    
    /**
     * Instantiates a new SqlConnectorParams.
     */
    public SqlConnectorParams()
    {
        this(null, null, true, -1);
    }
    
    /**
     * Instantiates a new SqlConnectorParams.
     *
     * @param rs the result set
     * @param cacheProvider the cache provider
     * @param silent the "is silent" flag
     * @param logStep the log step
     */
    public SqlConnectorParams(ResultSet rs,
            CacheProvider<String, Object> cacheProvider, boolean silent,
            int logStep)
    {
        _rs = rs;
        
        _fileName = null;
        _fileNameRequired = false;
        _fieldsMapping = null;
        
        _unique = false;
        _checkKeyField = false;
        _filterByField = null;
        _filterByFieldValue = null;
        
        _realFileName = null;
        _output = null;
        _tableName = null;
        
        setCacheProvider(cacheProvider);
        setSilent(silent);
        setLogStep(logStep);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.connector.DataSetConnectorParams#alias2alias(com.
     * toolsverse.etl.common.Alias)
     */
    @Override
    public Alias alias2alias(Alias source)
        throws Exception
    {
        Alias alias = new Alias();
        
        BeanUtils.copyProperties(alias, source);
        
        if (!source.isDbConnection() && !Utils.isNothing(source.getUrl()))
        {
            String url = FilenameUtils.getName(source.getUrl());
            
            alias.setUrl(url);
        }
        
        return alias;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.connector.DataSetConnectorParams#copy()
     */
    @Override
    public SqlConnectorParams copy()
    {
        SqlConnectorParams params = new SqlConnectorParams();
        
        params.setFileNameRequired(isFileNameRequired());
        params.setFileName(getFileName());
        params.setTableName(getTableName());
        params.setUseSelectedDataSet(useSelectedDataSet());
        
        return params;
    }
    
    /**
     * Gets the fields mapping. Used to map original field name to the new one. If some of the names are missing they will not be added to the data set.
     *
     * @return the fields mapping
     */
    public ListHashMap<String, String> getFieldsMapping()
    {
        return _fieldsMapping;
    }
    
    /**
     * Gets the file name.
     *
     * @return the file name
     */
    public String getFileName()
    {
        return _fileName;
    }
    
    /**
     * Gets the file name.
     *
     * @param name the name
     * @return the file name
     */
    public String getFileName(String name)
    {
        if (Utils.isNothing(_fileName))
        {
            return FileUtils.getFilename(name, SystemConfig.instance()
                    .getDataFolderName(), ".sql", false);
        }
        else
        {
            return FileUtils.getFilename(_fileName, SystemConfig.instance()
                    .getDataFolderName(), ".sql", false);
        }
    }
    
    /**
     * Gets the field name to filter by.
     *
     * @return the field name to filter by
     */
    public String getFilterByField()
    {
        return _filterByField;
    }
    
    /**
     * Gets the field value to filter by.
     *
     * @return the field value to filter by
     */
    public String getFilterByFieldValue()
    {
        return _filterByFieldValue;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnectorParams#getInitStr(java.lang
     * .String, com.toolsverse.etl.common.Alias)
     */
    @Override
    public String getInitStr(String name, Alias alias)
    {
        return FILE_NAME_PROP + "=\"" + alias.getUrl() + "\" "
                + Utils.str2PropsStr(alias.getParams(), " ", "\"");
    }
    
    /**
     * Gets the real file name.
     *
     * @return the real file name
     */
    public String getRealFileName()
    {
        return _realFileName;
    }
    
    /**
     * Gets the result set.
     *
     * @return the result set
     */
    public ResultSet getResultSet()
    {
        return _rs;
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
     * Gets the writer.
     *
     * @return the writer
     */
    public Writer getWriter()
    {
        return _output;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnectorParams#init(com.toolsverse
     * .etl.common.Alias)
     */
    @Override
    public void init(Alias alis)
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnectorParams#init(java.util.Map)
     */
    @Override
    public void init(Map<String, String> props)
    {
        if (props == null || props.size() == 0)
            return;
        
        setFileName(SystemConfig.instance().getPathUsingAppFolders(
                props.get(FILE_NAME_PROP)));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnectorParams#init(com.toolsverse
     * .util.ObjectStorage)
     */
    @Override
    public void init(ObjectStorage storage)
    {
        if (storage == null)
            return;
        
        setFileNameRequired(true);
        
        setFileName(SystemConfig.instance().getPathUsingAppFolders(
                storage.getString(PREFIX + FILE_NAME_PROP)));
        
        setTableName(storage.getString(PREFIX + TABLE_NAME_PROP));
        
        setUseSelectedDataSet(Utils.str2Boolean(
                storage.getString(PREFIX + USE_SELECTED_PROP), false));
    }
    
    /**
     * Checks if "check key field" flag was set.
     *
     * @return true, if "check key field" flag was set
     */
    public boolean isCheckKeyField()
    {
        return _checkKeyField;
    }
    
    /**
     * Checks if file name is required.
     *
     * @return true, if file name is required
     */
    public boolean isFileNameRequired()
    {
        return _fileNameRequired;
    }
    
    /**
     * Checks if is unique.
     *
     * @return true, if is unique
     */
    public boolean isUnique()
    {
        return _unique;
    }
    
    /**
     * Sets the "check key field: flag.
     *
     * @param value the new value for the "check key field" flag
     */
    public void setCheckKeyField(boolean value)
    {
        _checkKeyField = value;
    }
    
    /**
     * Sets the fields mapping.
     *
     * @param value the fields mapping
     */
    public void setFieldsMapping(ListHashMap<String, String> value)
    {
        _fieldsMapping = value;
    }
    
    /**
     * Sets the file name.
     *
     * @param value the new file name
     */
    public void setFileName(String value)
    {
        _fileName = value;
    }
    
    /**
     * Sets the "file name required" flag.
     *
     * @param value the new value for the "file name required" flag
     */
    public void setFileNameRequired(boolean value)
    {
        _fileNameRequired = value;
    }
    
    /**
     * Sets the field name to filter by.
     *
     * @param value the new field name to filter by
     */
    public void setFilterByField(String value)
    {
        _filterByField = value;
    }
    
    /**
     * Sets the field value to filter by.
     *
     * @param value the new field value to filter by
     */
    public void setFilterByFieldValue(String value)
    {
        _filterByFieldValue = value;
    }
    
    /**
     * Sets the real file name.
     *
     * @param value the new real file name
     */
    public void setRealFileName(String value)
    {
        _realFileName = value;
    }
    
    /**
     * Sets the result set.
     *
     * @param value the new result set
     */
    public void setResultSet(ResultSet value)
    {
        _rs = value;
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
     * Sets the "is unique" flag.
     *
     * @param value the new value for the "is unique" flag
     */
    public void setUnique(boolean value)
    {
        _unique = value;
    }
    
    /**
     * Sets the writer.
     *
     * @param value the new writer
     */
    public void setWriter(Writer value)
    {
        _output = value;
    }
    
}
