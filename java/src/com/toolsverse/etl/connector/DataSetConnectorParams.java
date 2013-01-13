/*
 * DataSetConnectorParams.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.toolsverse.cache.CacheProvider;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.sql.connection.TransactionMonitor;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.Utils;

/**
 * This data structure used by {@link com.toolsverse.etl.connector.DataSetConnector} to store parameters needed to persist and populate data set. 
 * The particular class implementing DataSetConnectorParams may include, for example, file name or database connection.  
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class DataSetConnectorParams implements Serializable
{
    
    /** The FILE NAME property. */
    public static final String FILE_NAME_PROP = "filename";
    
    /** The USE SELECTED property. */
    public static final String USE_SELECTED_PROP = "useselected";
    
    /** The MAX ROWS EXCEEDED EXCEPTION message. */
    public static final String MAX_ROWS_EXCEEDED_EXCEPTION = "Maximum allowed number of rows exceeded";
    
    /** The cache provider. */
    private CacheProvider<String, Object> _cacheProvider;
    
    /** The silent flag. */
    private boolean _silent;
    
    /** The log step flag. */
    private int _logStep;
    
    /** The before callback. */
    private BeforeCallback _beforeCallback;
    
    /** The add field value callback. */
    private AddFieldValueCallback _addFieldValueCallback;
    
    /** The add record callback. */
    private AddRecordCallback _addRecordCallback;
    
    /** The after callback. */
    private AfterCallback _afterCallback;
    
    /** The parameters. */
    private Map<String, String> _params;
    
    /** The pre persist occurred flag. */
    private boolean _prePersistOccurred;
    
    /** The post persist occurred flag. */
    private boolean _postPersistOccurred;
    
    /** The clean up persist occurred. */
    private boolean _cleanUpPersistOccurred;
    
    /** The maximum number of rows allowed. */
    private int _maxRows;
    
    /** The input stream. */
    private InputStream _inputStream;
    
    /** The output stream. */
    private OutputStream _outputStream;
    
    /** The close input stream flag. */
    private boolean _closeInput;
    
    /** The close output stream flag. */
    private boolean _closeOutput;
    
    /** The transaction monitor. */
    private TransactionMonitor _transactionMonitor;
    
    /** If true the 'selected' data set must be used. */
    private boolean _useSelectedDataSet;
    
    /**
     * Instantiates a new DataSetConnectorParams.
     */
    public DataSetConnectorParams()
    {
        _cacheProvider = null;
        _silent = true;
        _logStep = -1;
        _beforeCallback = null;
        _addFieldValueCallback = null;
        _addRecordCallback = null;
        _afterCallback = null;
        _params = new HashMap<String, String>();
        _prePersistOccurred = false;
        _postPersistOccurred = false;
        _cleanUpPersistOccurred = false;
        _maxRows = -1;
        _inputStream = null;
        _outputStream = null;
        _closeInput = true;
        _closeOutput = true;
        _useSelectedDataSet = false;
    }
    
    /**
     * Creates an new Alias object using given source alias.
     *
     * @param source the source alias
     * @return the alias
     * @throws Exception in case of any error
     */
    public abstract Alias alias2alias(Alias source)
        throws Exception;
    
    /**
     * Create a copy of this object.
     *
     * @return the copy of this object
     */
    public abstract DataSetConnectorParams copy();
    
    /**
     * Gets the AddFieldValueCallback object.
     *
     * @return the AddFieldValueCallback object
     */
    public AddFieldValueCallback getAddFieldValueCallback()
    {
        return _addFieldValueCallback;
    }
    
    /**
     * Gets the AddRecordCallback object.
     *
     * @return the AddRecordCallback object
     */
    public AddRecordCallback getAddRecordCallback()
    {
        return _addRecordCallback;
    }
    
    /**
     * Gets the AfterCallback object.
     *
     * @return the AfterCallback object
     */
    public AfterCallback getAfterCallback()
    {
        return _afterCallback;
    }
    
    /**
     * Gets the BeforeCallback object.
     *
     * @return the BeforeCallback object
     */
    public BeforeCallback getBeforeCallback()
    {
        return _beforeCallback;
    }
    
    /**
     * Gets the cache provider.
     *
     * @return the cache provider
     */
    public CacheProvider<String, Object> getCacheProvider()
    {
        return _cacheProvider;
    }
    
    /**
     * Gets the initialization string using given alias. Used by ETL framework.
     *
     * @param name the name
     * @param alias the alias
     * @return the initialization string
     */
    public abstract String getInitStr(String name, Alias alias);
    
    /**
     * Gets the input stream.
     *
     * @return the input stream
     */
    public InputStream getInputStream()
    {
        return _inputStream;
    }
    
    /**
     * Gets the log step. The log step is a number of "lines processed" before calling Logger.log(...)
     *
     * @return the log step
     */
    public int getLogStep()
    {
        return _logStep;
    }
    
    /**
     * Gets the maximum number of rows allowed for the DataSet. Once number of rows reaches maximum - data set population stops. 
     *
     * @return the maximum number of rows for the DataSet
     */
    public int getMaxRows()
    {
        return _maxRows;
    }
    
    /**
     * Gets the output stream.
     *
     * @return the output stream
     */
    public OutputStream getOutputStream()
    {
        return _outputStream;
    }
    
    /**
     * Gets the parameters. Parameters stored as a map of name\value pairs. 
     *
     * @return the parameters
     */
    public Map<String, String> getParams()
    {
        return _params;
    }
    
    /**
     * Gets the transaction monitor.
     *
     * @return the transaction monitor
     */
    public TransactionMonitor getTransactionMonitor()
    {
        return _transactionMonitor;
    }
    
    /**
     * Initializes itself using given alias.
     *
     * @param alias the alias
     */
    public abstract void init(Alias alias);
    
    /**
     * Initializes itself using given properties.
     *
     * @param props the properties
     */
    public abstract void init(Map<String, String> props);
    
    /**
     * Initializes itself using given object storage.
     *
     * @param storage the object storage
     */
    public abstract void init(ObjectStorage storage);
    
    /**
     * Checks if DataSetConnector#cleanUp method has been already called.
     *
     * @return true, if DataSetConnector#cleanUp method has been already called
     */
    public boolean isCleanUpPersistOccured()
    {
        return _cleanUpPersistOccurred;
    }
    
    /**
     * Checks if input stream needs to be closed.
     *
     * @return true, if input stream needs to be closed
     */
    public boolean isCloseInput()
    {
        return _closeInput;
    }
    
    /**
     * Checks if output stream needs to be closed.
     *
     * @return true, if output stream needs to be closed
     */
    public boolean isCloseOutput()
    {
        return _closeOutput;
    }
    
    /**
     * Checks if is "max rows exceeded" exception has occurred.
     *
     * @param ex the exception
     * @return true, if is "max rows exceeded" exception has occurred
     */
    public boolean isMaxRowsExceededException(Exception ex)
    {
        return Utils.isParticularException(ex, MAX_ROWS_EXCEEDED_EXCEPTION);
    }
    
    /**
     * Checks if DataSetConnector#postPersist has been already called.
     *
     * @return true, if DataSetConnector#postPersist has been already called
     */
    public boolean isPostPersistOccured()
    {
        return _postPersistOccurred;
    }
    
    /**
     * Checks if DataSetConnector#prePersist has been already called.
     *
     * @return true, if DataSetConnector#prePersist has been already called
     */
    public boolean isPrePersistOccured()
    {
        return _prePersistOccurred;
    }
    
    /**
     * Checks if "is silent" flag set. If it is the DataSetConnector should not log any intermediate state.
     *
     * @return true, if "is silent" flag set
     */
    public boolean isSilent()
    {
        return _silent;
    }
    
    /**
     * Sets the AddFieldValueCallback object.
     *
     * @param value the new AddFieldValueCallback object
     */
    public void setAddFieldValueCallback(AddFieldValueCallback value)
    {
        _addFieldValueCallback = value;
    }
    
    /**
     * Sets the AddRecordCallback object.
     *
     * @param value the new AddRecordCallback object
     */
    public void setAddRecordCallback(AddRecordCallback value)
    {
        _addRecordCallback = value;
    }
    
    /**
     * Sets the AfterCallback object.
     *
     * @param value the new AfterCallback object
     */
    public void setAfterCallback(AfterCallback value)
    {
        _afterCallback = value;
    }
    
    /**
     * Sets the AfterCallback object.
     *
     * @param value the new AfterCallback object
     */
    public void setBeforeCallback(BeforeCallback value)
    {
        _beforeCallback = value;
    }
    
    /**
     * Sets the cache provider.
     *
     * @param value the new cache provider
     */
    public void setCacheProvider(CacheProvider<String, Object> value)
    {
        _cacheProvider = value;
    }
    
    /**
     * Sets the flag "clean up has occurred".
     *
     * @param value the new value for the flag "clean up has occurred"
     */
    public void setCleanUpPersistOccurred(boolean value)
    {
        _cleanUpPersistOccurred = value;
    }
    
    /**
     * Sets the flag "close input stream".
     *
     * @param value the new value for the flag "close input stream"
     */
    public void setCloseInput(boolean value)
    {
        _closeInput = value;
    }
    
    /**
     * Sets the flag "close output stream".
     *
     * @param value the new value for the flag "close output stream"
     */
    public void setCloseOutput(boolean value)
    {
        _closeOutput = value;
    }
    
    /**
     * Sets the input stream.
     *
     * @param value the new input stream
     */
    public void setInputStream(InputStream value)
    {
        _inputStream = value;
    }
    
    /**
     * Sets the log step.
     *
     * @param value the new log step
     */
    public void setLogStep(int value)
    {
        _logStep = value;
    }
    
    /**
     * Sets the maximum number of rows allowed for the DataSet.
     *
     * @param value the new the maximum number of rows allowed for the DataSet
     */
    public void setMaxRows(int value)
    {
        _maxRows = value;
    }
    
    /**
     * Sets the output stream.
     *
     * @param value the new output stream
     */
    public void setOutputStream(OutputStream value)
    {
        _outputStream = value;
    }
    
    /**
     * Sets the flag "post persist has occurred".
     *
     * @param value the new value for the flag "post persist has occurred"
     */
    public void setPostPersistOccured(boolean value)
    {
        _postPersistOccurred = value;
    }
    
    /**
     * Sets the flag "pre persist has occurred".
     *
     * @param value the new value for the flag "pre persist has occurred"
     */
    public void setPrePersistOccured(boolean value)
    {
        _prePersistOccurred = value;
    }
    
    /**
     * Sets the "is silent" flag.
     *
     * @param value the new value for the "is silent" flag
     */
    public void setSilent(boolean value)
    {
        _silent = value;
    }
    
    /**
     * Sets the transaction monitor.
     *
     * @param value the new transaction monitor
     */
    public void setTransactionMonitor(TransactionMonitor value)
    {
        _transactionMonitor = value;
    }
    
    /**
     * Sets the "use selected data set" flag.
     *
     * @param value the new value for the "use selected data set" flag
     */
    public void setUseSelectedDataSet(boolean value)
    {
        _useSelectedDataSet = value;
    }
    
    /**
     * Checks if "use selected data set" flag is set.
     *
     * @return true, if successful
     */
    public boolean useSelectedDataSet()
    {
        return _useSelectedDataSet;
    }
    
}
