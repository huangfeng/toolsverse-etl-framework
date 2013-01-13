/*
 * FileConnectorParams.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.beanutils.BeanUtils;

import com.toolsverse.cache.CacheProvider;
import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.Utils;

/**
 * The abstract {@link DataSetConnectorParams} implementation used by file based DataSetConnectors.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class FileConnectorParams extends DataSetConnectorParams
{
    /** The SPLIT BY property. */
    public static final String SPLIT_BY_PROP = "splitby";
    
    /** The folder. */
    private String _folder;
    
    /** The file name. */
    private String _fileName;
    
    /** The date format. */
    private String _dateFormat;
    
    /** The time format. */
    private String _timeFormat;
    
    /** The date time format. */
    private String _dateTimeFormat;
    
    /** The file name required flag. */
    private boolean _fileNameRequired;
    
    /** The real file name. */
    private String _realFileName;
    
    /** If this property is not null DataSetConnector must split data set on multiple data set using <code>_splitBy</code> as a key. */
    private String _splitBy;
    
    /**
     * Instantiates a new FileConnectorParams.
     *
     * @param cacheProvider the cache provider
     * @param silent the silent flag
     * @param logStep the log step
     */
    public FileConnectorParams(CacheProvider<String, Object> cacheProvider,
            boolean silent, int logStep)
    {
        setCacheProvider(cacheProvider);
        setSilent(silent);
        setLogStep(logStep);
        
        _folder = null;
        _fileName = null;
        _dateFormat = null;
        _timeFormat = null;
        _dateTimeFormat = null;
        
        _fileNameRequired = false;
        
        _realFileName = null;
        
        _splitBy = null;
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
    
    /**
     * Gets the date format.
     *
     * @return the date format
     */
    public String getDateFormat()
    {
        return !Utils.isNothing(_dateFormat) ? _dateFormat
                : DataSet.DATA_SET_DATE_TIME_FORMAT;
    }
    
    /**
     * Gets the date time format.
     *
     * @return the date time format
     */
    public String getDateTimeFormat()
    {
        return !Utils.isNothing(_dateTimeFormat) ? _dateTimeFormat
                : DataSet.DATA_SET_DATE_TIME_FORMAT;
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
     * @param name the original file name
     * @param ext the extension
     * @param forceExt if true the given extension will be used
     * @return the file name
     */
    public String getFileName(String name, String ext, boolean forceExt)
    {
        if (Utils.isNothing(_fileName))
            return FileUtils.getFilename(name, getFolder(), ext, forceExt);
        else
            return FileUtils.getFilename(_fileName, getFolder(), ext, forceExt);
    }
    
    /**
     * Gets the folder.
     *
     * @return the folder
     */
    public String getFolder()
    {
        return !Utils.isNothing(_folder) ? _folder : SystemConfig.instance()
                .getDataFolderName();
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
     * Gets the split by property. If this property is not null DataSetConnector must split data set on multiple data 
     * set using <code>splitBy</code> as a key.
     *
     * @return the split by property
     */
    public String getSplitBy()
    {
        return _splitBy;
    }
    
    /**
     * Gets the time format.
     *
     * @return the time format
     */
    public String getTimeFormat()
    {
        return !Utils.isNothing(_timeFormat) ? _timeFormat
                : DataSet.DATA_SET_TIME_FORMAT;
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
        if (!alis.isDbConnection())
        {
            
            Boolean hasWildcard = FileUtils.hasWildCard(alis.getUrl());
            
            if (hasWildcard != null && !hasWildcard)
                setFileName(alis.getUrl());
        }
        
        Properties props = Utils.getProperties(alis.getParams());
        
        if (props == null || props.size() == 0)
            return;
        
        setDateFormat(props.getProperty(SqlUtils.DATE_FORMAT_PROP));
        setTimeFormat(props.getProperty(SqlUtils.TIME_FORMAT_PROP));
        setDateTimeFormat(props.getProperty(SqlUtils.DATE_TIME_FORMAT_PROP));
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
        
        setDateFormat(props.get(SqlUtils.DATE_FORMAT_PROP));
        setTimeFormat(props.get(SqlUtils.TIME_FORMAT_PROP));
        setDateTimeFormat(props.get(SqlUtils.DATE_TIME_FORMAT_PROP));
    }
    
    /**
     * Checks if given format string is a date format.
     *
     * @param format the format
     * @return true, if is date format
     */
    public boolean isDateFormat(String format)
    {
        return getDateFormat().equals(format);
    }
    
    /**
     * Checks if given format string is a date time format.
     *
     * @param format the format
     * @return true, if is date time format
     */
    public boolean isDateTimeFormat(String format)
    {
        return getDateTimeFormat().equals(format);
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
     * Checks if given format string is a time format.
     *
     * @param format the format
     * @return true, if is time format
     */
    public boolean isTimeFormat(String format)
    {
        return getTimeFormat().equals(format);
    }
    
    /**
     * Sets the date format.
     *
     * @param value the new date format
     */
    public void setDateFormat(String value)
    {
        _dateFormat = value;
        
        getParams().put(SqlUtils.DATE_FORMAT_PROP, value);
    }
    
    /**
     * Sets the date time format.
     *
     * @param value the new date time format
     */
    public void setDateTimeFormat(String value)
    {
        _dateTimeFormat = value;
        
        getParams().put(SqlUtils.DATE_TIME_FORMAT_PROP, value);
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
     * Sets the folder.
     *
     * @param value the new folder
     */
    public void setFolder(String value)
    {
        _folder = value;
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
     * Sets the split by property. If this property is not null DataSetConnector must split data set on multiple data 
     * set using <code>splitBy</code> as a key.
     *
     * @param value the new value of the split by property
     */
    public void setSplitBy(String value)
    {
        _splitBy = value;
    }
    
    /**
     * Sets the time format.
     *
     * @param value the new time format
     */
    public void setTimeFormat(String value)
    {
        _timeFormat = value;
        
        getParams().put(SqlUtils.TIME_FORMAT_PROP, value);
    }
    
}
