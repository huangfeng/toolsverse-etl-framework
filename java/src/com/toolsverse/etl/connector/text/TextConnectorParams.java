/*
 * TextConnectorParams.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.text;

import java.io.Writer;
import java.util.Map;
import java.util.Properties;

import com.toolsverse.cache.CacheProvider;
import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.connector.DataSetConnectorParams;
import com.toolsverse.etl.connector.FileConnectorParams;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.Utils;

/**
 * The {@link DataSetConnectorParams} used by TextConnector.
 * 
 * @see com.toolsverse.etl.connector.text.TextConnector
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class TextConnectorParams extends FileConnectorParams
{
    
    /** The Constant UNIX_LINE_SEPARATOR. */
    public static final String UNIX_LINE_SEPARATOR = "u";
    
    /** The Constant WINDOWS_LINE_SEPARATOR. */
    public static final String WINDOWS_LINE_SEPARATOR = "w";
    
    /** The Constant SYSTEM_LINE_SEPARATOR. */
    public static final String SYSTEM_LINE_SEPARATOR = "s";
    
    /** The PREFIX. */
    public static final String PREFIX = "text";
    
    /** The LINE SEPARATOR property. */
    public static final String LINESEPARATOR_PROP = "lineseparator";
    
    /** The DELIMETER property. */
    public static final String DELIMETER_PROP = "delimiter";
    
    /** The FIELDS property. */
    public static final String FIELDS_PROP = "fields";
    
    /** The METADATA property. */
    public static final String METADATA_PROP = "metadata";
    
    /** The FIRST ROW HAS DATA property. */
    public static final String FIRST_ROW_DATA_PROP = "firstrow";
    
    /** The delimiter. */
    private String _delimiter;
    
    /** The fields. */
    private String _fields;
    
    /** The "persist meta data" flag. */
    private boolean _persistMetaData;
    
    /** The  file extension. */
    private String _ext;
    
    /** The "first row hash data" flag. */
    private boolean _firstRowData;
    
    /** The _line separator. */
    private String _lineSeparator;
    
    /** The writer. */
    private Writer _output;
    
    /** The length array. */
    private int[] _lengthArray;
    
    /**
     * Instantiates a new TextConnectorParams.
     */
    public TextConnectorParams()
    {
        this(null, true, -1, null, true);
    }
    
    /**
     * Instantiates a new TextConnectorParams.
     *
     * @param cacheProvider the cache provider
     * @param silent the "is silent" flag
     * @param logStep the log step
     */
    public TextConnectorParams(CacheProvider<String, Object> cacheProvider,
            boolean silent, int logStep)
    {
        this(cacheProvider, silent, logStep, null, true);
    }
    
    /**
     * Instantiates a new TextConnectorParams.
     *
     * @param cacheProvider the cache provider
     * @param silent the "is silent" flag
     * @param logStep the log step
     * @param delimiter the delimiter
     * @param persistMetaData the "persist meta data" flag
     */
    public TextConnectorParams(CacheProvider<String, Object> cacheProvider,
            boolean silent, int logStep, String delimiter,
            boolean persistMetaData)
    {
        super(cacheProvider, silent, logStep);
        
        _ext = null;
        _delimiter = delimiter;
        _persistMetaData = persistMetaData;
        _firstRowData = true;
        _fields = null;
        _lengthArray = null;
        _output = null;
        _lineSeparator = SYSTEM_LINE_SEPARATOR;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.connector.DataSetConnectorParams#copy()
     */
    @Override
    public DataSetConnectorParams copy()
    {
        TextConnectorParams params = new TextConnectorParams();
        
        params.setFileNameRequired(isFileNameRequired());
        params.setFileName(getFileName());
        params.setDelimiter(getDelimiter());
        params.setLineSeparator(getLineSeparator());
        params.setFields(getFields());
        params.setPersistMetaData(isPersistMetaData());
        params.setFirstRowData(isFirstRowData());
        params.setCharSeparator(getCharSeparator());
        params.setDateFormat(getDateFormat());
        params.setDateTimeFormat(getDateTimeFormat());
        params.setTimeFormat(getTimeFormat());
        params.setUseSelectedDataSet(useSelectedDataSet());
        params.setSplitBy(getSplitBy());
        
        return params;
    }
    
    /**
     * Gets the char separator.
     *
     * @return the char separator
     */
    public String getCharSeparator()
    {
        String value = getParams().get(SqlUtils.CHAR_SEPARATOR_PROP);
        
        return value != null ? value : "";
    }
    
    /**
     * gets the delimiter. Some characters are allowed, some not.
     * @param delimiter the source
     * @return the delimiter
     */
    private String getDelim(String delimiter)
    {
        if (" ".equals(delimiter) || "\t".equals(delimiter)
                || !Utils.isNothing(delimiter))
            return delimiter;
        else
            return SqlUtils.DEFAULT_DELIMITER;
    }
    
    /**
     * Gets the delimiter.
     *
     * @return the delimiter
     */
    public String getDelimiter()
    {
        return _delimiter;
    }
    
    /**
     * Gets the file extension. Default is ".dat".
     *
     * @return the file extension
     */
    public String getExt()
    {
        return !Utils.isNothing(_ext) ? _ext : ".dat";
    }
    
    /**
     * Gets the comma delimited string of field names. 
     *
     * @return the fields
     */
    public String getFields()
    {
        return _fields;
    }
    
    /**
     * Gets the length array.
     *
     * @return the length array
     */
    public int[] getLengthArray()
    {
        return _lengthArray;
    }
    
    /**
     * Gets the line separator.
     *
     * @return the line separator
     */
    public String getLineSeparator()
    {
        if (SYSTEM_LINE_SEPARATOR.equalsIgnoreCase(_lineSeparator))
        {
            return Utils.NEWLINE;
        }
        else if (WINDOWS_LINE_SEPARATOR.equalsIgnoreCase(_lineSeparator))
        {
            return "\r\n";
        }
        else if (UNIX_LINE_SEPARATOR.equalsIgnoreCase(_lineSeparator))
        {
            return "\n";
        }
        else
            return _lineSeparator;
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
     * com.toolsverse.etl.connector.FileConnectorParams#init(com.toolsverse.
     * etl.common.Alias)
     */
    @Override
    public void init(Alias alis)
    {
        super.init(alis);
        
        Properties props = Utils.getProperties(alis.getParams());
        
        if (props == null || props.size() == 0)
        {
            setDelimiter(SqlUtils.DEFAULT_DELIMITER);
            
            return;
        }
        
        String delimiter = props.getProperty(DELIMETER_PROP);
        setDelimiter(getDelim(delimiter));
        
        setLineSeparator(props.getProperty(LINESEPARATOR_PROP));
        
        setPersistMetaData(Utils.str2Boolean(props.getProperty(METADATA_PROP),
                true));
        setFirstRowData(Utils.str2Boolean(
                props.getProperty(FIRST_ROW_DATA_PROP), true));
        
        setFields(props.getProperty(FIELDS_PROP));
        
        setCharSeparator(props.getProperty(SqlUtils.CHAR_SEPARATOR_PROP));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.connector.FileConnectorParams#init(java.util.Map)
     */
    @Override
    public void init(Map<String, String> props)
    {
        if (props == null || props.size() == 0)
        {
            setDelimiter(SqlUtils.DEFAULT_DELIMITER);
            
            return;
        }
        
        super.init(props);
        
        String delimiter = props.get(DELIMETER_PROP);
        setDelimiter(getDelim(delimiter));
        
        setLineSeparator(props.get(LINESEPARATOR_PROP));
        
        setPersistMetaData(Utils.str2Boolean(props.get(METADATA_PROP), true));
        setFirstRowData(Utils.str2Boolean(props.get(FIRST_ROW_DATA_PROP), true));
        
        setCharSeparator(props.get(SqlUtils.CHAR_SEPARATOR_PROP));
        
        setFields(props.get(FIELDS_PROP));
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
        
        String delimiter = storage.getString(PREFIX + DELIMETER_PROP);
        setDelimiter(getDelim(delimiter));
        
        setLineSeparator(storage.getString(PREFIX + LINESEPARATOR_PROP));
        
        setFields(storage.getString(PREFIX + FIELDS_PROP));
        setPersistMetaData(Utils.str2Boolean(
                storage.getString(PREFIX + METADATA_PROP), true));
        setFirstRowData(Utils.str2Boolean(
                storage.getString(PREFIX + FIRST_ROW_DATA_PROP), true));
        
        setCharSeparator(storage.getString(PREFIX
                + SqlUtils.CHAR_SEPARATOR_PROP));
        
        setDateFormat(storage.getString(PREFIX + SqlUtils.DATE_FORMAT_PROP));
        setTimeFormat(storage.getString(PREFIX + SqlUtils.TIME_FORMAT_PROP));
        setDateTimeFormat(storage.getString(PREFIX
                + SqlUtils.DATE_TIME_FORMAT_PROP));
        
        setUseSelectedDataSet(Utils.str2Boolean(
                storage.getString(PREFIX + USE_SELECTED_PROP), false));
        
        setSplitBy(storage.getString(PREFIX + SPLIT_BY_PROP));
    }
    
    /**
     * Checks if "first row has data" flag was set.
     *
     * @return true, if "first row has data" flag was set
     */
    public boolean isFirstRowData()
    {
        return _firstRowData;
    }
    
    /**
     * Checks if "is persist meta data" flag was set.
     *
     * @return true, "is persist meta data" flag was set
     */
    public boolean isPersistMetaData()
    {
        return _persistMetaData;
    }
    
    public void setCharSeparator(String value)
    {
        if (Utils.isNothing(value))
        {
            getParams().put(SqlUtils.CHAR_SEPARATOR_PROP, "");
            
            return;
        }
        
        getParams().put(SqlUtils.CHAR_SEPARATOR_PROP,
                String.valueOf((value.trim().charAt(0))));
    }
    
    /**
     * Sets the delimiter.
     *
     * @param value the new delimiter
     */
    public void setDelimiter(String value)
    {
        _delimiter = value;
    }
    
    /**
     * Sets the file extension.
     *
     * @param value the new file extension
     */
    public void setExt(String value)
    {
        _ext = value;
    }
    
    /**
     * Sets the fields.
     *
     * @param value the new fields
     */
    public void setFields(String value)
    {
        _fields = value;
    }
    
    /**
     * Sets the "first row hash data" flag.
     *
     * @param value the new value for the "first row has data" flag
     */
    public void setFirstRowData(boolean value)
    {
        _firstRowData = value;
    }
    
    /**
     * Sets the length array.
     *
     * @param value the new length array
     */
    public void setLengthArray(int[] value)
    {
        _lengthArray = value;
    }
    
    /**
     * Sets the line separator.
     *
     * @param value the new line separator
     */
    public void setLineSeparator(String value)
    {
        if (Utils.isNothing(value))
        {
            _lineSeparator = SYSTEM_LINE_SEPARATOR;
            
            return;
        }
        
        _lineSeparator = value.trim();
        
        if (!Utils.belongsTo(new String[] {SYSTEM_LINE_SEPARATOR,
                UNIX_LINE_SEPARATOR, WINDOWS_LINE_SEPARATOR}, _lineSeparator))
            _lineSeparator = SYSTEM_LINE_SEPARATOR;
        
    }
    
    /**
     * Sets the "persist meta data" flag.
     *
     * @param value the new value for the "persist meta data" flag
     */
    public void setPersistMetaData(boolean value)
    {
        _persistMetaData = value;
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
