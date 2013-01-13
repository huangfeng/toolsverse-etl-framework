/*
 * TextConnector.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.text;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetData;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.connector.BaseDataSetConnector;
import com.toolsverse.etl.connector.ConnectorResource;
import com.toolsverse.etl.connector.ConnectorResult;
import com.toolsverse.etl.connector.DataSetConnector;
import com.toolsverse.etl.connector.DataSetConnectorParams;
import com.toolsverse.etl.connector.FileConnectorResource;
import com.toolsverse.etl.connector.xml.XmlConnector;
import com.toolsverse.etl.connector.xml.XmlConnectorParams;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * Reads and writes delimited and fixed-length text files. Supports data streaming.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class TextConnector extends BaseDataSetConnector implements
        DataSetConnector<TextConnectorParams, ConnectorResult>
{
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#cleanUp(com.toolsverse.
     * etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public void cleanUp(TextConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        if (params.getWriter() != null)
            params.getWriter().close();
        
        if (params.getOutputStream() != null && params.isCloseOutput())
            params.getOutputStream().close();
        
        params.setCleanUpPersistOccurred(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#getDataSetConnectorParams()
     */
    public TextConnectorParams getDataSetConnectorParams()
    {
        return new TextConnectorParams();
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
    
    /**
     * Gets the array of lengths for each field in the fixed-length file.
     *
     * @param params the TextConnectorParams
     * @param fieldCount the field count
     * @return the length array
     */
    private int[] getLengthArray(TextConnectorParams params, int fieldCount)
    {
        int[] lengthArray = null;
        
        // fixed length file
        if (!Utils.isNothing(params.getFields()))
        {
            String[] fiedls = params.getFields().split(
                    Utils.str2Regexp(params.getDelimiter()));
            
            if (fieldCount < 0)
                fieldCount = fiedls.length;
            
            if (fiedls.length < fieldCount || fieldCount <= 0)
                throw new IllegalArgumentException(
                        Resource.WRONG_ARGUMENT_VALUE.getValue() + ":fields");
            
            lengthArray = new int[fieldCount];
            
            for (int i = 0; i < fieldCount; i++)
            {
                if (!Utils.isUnsignedInt(fiedls[i]))
                    throw new IllegalArgumentException(
                            Resource.WRONG_ARGUMENT_VALUE.getValue()
                                    + ":fields");
                
                lengthArray[i] = Utils.str2Int(fiedls[i], 0);
            }
        }
        
        return lengthArray;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getLicensePropertyName()
     */
    public String getLicensePropertyName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.connector.DataSetConnector#getName()
     */
    public String getName()
    {
        return "Text";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getVendor()
     */
    public String getVendor()
    {
        return SystemConfig.DEFAULT_VENDOR;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getVersion()
     */
    public String getVersion()
    {
        return "3.1";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.BaseExtension#getXmlConfigFileName()
     */
    @Override
    public String getXmlConfigFileName()
    {
        return "com/toolsverse/etl/connector/text/text_config.xml";
    }
    
    /**
     * Gets the xml connector.
     *
     * @return the xml connector
     */
    private XmlConnector getXmlConnector()
    {
        return new XmlConnector();
    }
    
    /**
     * Gets the XmlConnectorParams using given TextConnectorParams. Used to create meta data in xml format.
     *
     * @param params the TextConnectorParams
     * @return the XmlConnectorParams
     */
    private XmlConnectorParams getXmlParams(TextConnectorParams params)
    {
        XmlConnectorParams xmlConnectorParams = new XmlConnectorParams(
                params.getCacheProvider(), params.isSilent(),
                params.getLogStep());
        
        xmlConnectorParams.setFileName(params.getFileName());
        xmlConnectorParams.setFolder(params.getFolder());
        
        return xmlConnectorParams;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#inlinePersist(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver,
     * com.toolsverse.etl.common.DataSetRecord, int, int)
     */
    public void inlinePersist(TextConnectorParams params, DataSet dataSet,
            Driver driver, DataSetRecord record, int row, int records)
        throws Exception
    {
        if (record == null)
            return;
        
        String recordS = "";
        
        int colCount = dataSet.getFieldCount();
        
        for (int col = 0; col < colCount; col++)
        {
            FieldDef fieldDef = dataSet.getFieldDef(col);
            
            if (!fieldDef.isVisible())
                continue;
            
            Object fieldValue = record.get(col);
            
            String value = dataSet.encode(fieldDef, fieldValue, driver,
                    params.getParams());
            
            if (Utils.isNothing(value) || "null".equalsIgnoreCase(value))
                value = "";
            else if (fieldDef.isChar())
                value = params.getCharSeparator() + value
                        + params.getCharSeparator();
            
            if (params.getLengthArray() == null)
            {
                if (record.size() > 1 && col > 0)
                    recordS = recordS + params.getDelimiter() + value;
                else
                    recordS = recordS + value;
            }
            else
            {
                recordS = recordS
                        + Utils.padRight(value, params.getLengthArray()[col],
                                true);
            }
        }
        
        if (row >= 0 && records >= 0 && !params.isSilent()
                && params.getLogStep() > 0 && (row % params.getLogStep()) == 0)
            Logger.log(Logger.INFO, EtlLogger.class, dataSet.getName() + ": "
                    + EtlResource.PERSITING_RECORD.getValue() + row
                    + " out of " + records);
        
        params.getWriter().write(recordS.replaceAll("(\\r|\\n)", ""));
        params.getWriter().write(params.getLineSeparator());
    }
    
    /**
     * Parses the line of text and updates current record of the data set.
     *
     * @param dataSet the data set
     * @param driver the driver
     * @param line the line to parse
     * @param params the TextConnectorParams
     * @param delimiter the delimiter
     * @param index the row index
     * @param hasKey the "has key" flag
     * @param hasMetadata the "has meta data" flag
     * @param types the types
     * @param lengthArray the length array
     * @throws Exception in case of any error
     */
    private void parseLine(DataSet dataSet, Driver driver, String line,
            TextConnectorParams params, String delimiter, int index,
            boolean hasKey, boolean hasMetadata, Map<Integer, Boolean> types,
            int[] lengthArray)
        throws Exception
    {
        Object[] values = Utils.split(line, delimiter, lengthArray);
        
        DataSetRecord record = new DataSetRecord();
        
        if (!params.isSilent() && params.getLogStep() > 0
                && (index % params.getLogStep()) == 0)
            Logger.log(Logger.INFO, EtlLogger.class, dataSet.getName() + ": "
                    + index + EtlResource.READING_DATASET_MSG.getValue());
        
        int fCount = values != null ? values.length : 0;
        FieldDef fieldDef;
        String value;
        Object fldValue;
        TypedKeyValue<Integer, Object> typeAndValue;
        
        for (int col = 0; col < fCount; col++)
        {
            value = Utils.makeString(values[col]);
            
            if (hasMetadata)
            {
                if (!params.isFirstRowData() && index == 1)
                    return;
                
                fieldDef = dataSet.getFieldDef(col);
                
                fldValue = dataSet.decode(fieldDef,
                        SqlUtils.getUnquotedText(params.getParams(), value),
                        driver, params.getParams());
            }
            else
            {
                if (index == 1)
                {
                    if (!params.isFirstRowData())
                    {
                        fieldDef = new FieldDef();
                        
                        if (!Utils.isNothing(value))
                        {
                            typeAndValue = SqlUtils.getTypeAndValue(value,
                                    params.getParams());
                            
                            fieldDef.setName(typeAndValue.getValue().toString());
                        }
                        else
                            fieldDef.setName("field" + (col + 1));
                        
                        fieldDef.setSqlDataType(Types.VARCHAR);
                        fieldDef.setNativeDataType("VARCHAR");
                        dataSet.addField(fieldDef);
                        
                        continue;
                    }
                    else
                    {
                        typeAndValue = SqlUtils.getTypeAndValue(value,
                                params.getParams());
                        
                        fldValue = typeAndValue.getValue();
                        
                        fieldDef = new FieldDef();
                        fieldDef.setName("field" + (col + 1));
                        
                        if (!Utils.isEmpty(fldValue))
                        {
                            fieldDef.setSqlDataType(typeAndValue.getKey());
                            fieldDef.setNativeDataType(driver.getType(
                                    new FieldDef(typeAndValue.getKey(), null),
                                    null, null));
                            
                            types.put(col, true);
                        }
                        else
                        {
                            fieldDef.setSqlDataType(Types.VARCHAR);
                            fieldDef.setNativeDataType("VARCHAR");
                        }
                        
                        dataSet.addField(fieldDef);
                        
                    }
                }
                else
                {
                    fieldDef = dataSet.getFieldDef(col);
                    
                    typeAndValue = SqlUtils.getTypeAndValue(value,
                            params.getParams());
                    
                    fldValue = typeAndValue.getValue();
                    
                    if (!Utils.isEmpty(fldValue))
                    {
                        fieldDef.setSqlDataType(SqlUtils.getFieldType(
                                typeAndValue.getKey(),
                                fieldDef.getSqlDataType(),
                                types.containsKey(col)));
                        fieldDef.setNativeDataType(driver.getType(new FieldDef(
                                fieldDef.getSqlDataType(), null), null, null));
                        
                        types.put(col, true);
                    }
                }
            }
            
            if (fldValue != null && "".equals(fldValue.toString()))
                fldValue = null;
            
            addValue(fldValue, record, dataSet);
            
            if (params.getAddFieldValueCallback() != null)
                params.getAddFieldValueCallback().onAddFieldValue(dataSet,
                        driver, record, fieldDef);
        }
        
        if (index == 1 && !params.isFirstRowData())
            return;
        
        if (params.getMaxRows() >= 0
                && dataSet.getRecordCount() >= params.getMaxRows())
        {
            throw new Exception(
                    DataSetConnectorParams.MAX_ROWS_EXCEEDED_EXCEPTION);
        }
        
        boolean added = dataSet.addRecord(record);
        
        if (added && params.getAddRecordCallback() != null)
            params.getAddRecordCallback().onAddRecord(dataSet, driver, record,
                    params.isFirstRowData() ? index : index - 1);
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#persist(com.toolsverse.
     * etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult persist(TextConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        if (dataSet == null
                || params == null
                || (driver == null && dataSet.getDriver() == null)
                || Utils.isNothing(dataSet.getName())
                || dataSet.getFieldCount() == 0
                || (params.isFileNameRequired() && Utils.isNothing(params
                        .getFileName())))
        {
            ConnectorResult result = new ConnectorResult();
            result.setRetCode(ConnectorResult.VALIDATION_FAILED_CODE);
            
            if (dataSet == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NULL
                        .getValue());
            if (driver == null && dataSet.getDriver() == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DRIVER_NULL
                        .getValue());
            if (params == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_PARAMS_NULL
                        .getValue());
            if (dataSet != null && dataSet.getFieldCount() == 0)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NO_FIELDS
                        .getValue());
            if (dataSet != null && Utils.isNothing(dataSet.getName()))
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NO_NAME
                        .getValue());
            
            if (params.isFileNameRequired()
                    && Utils.isNothing(params.getFileName()))
                result.addResult(FileConnectorResource.VALIDATION_ERROR_FILE_NAME_NOT_SPECIFIED
                        .getValue());
            
            return result;
        }
        
        if (!params.isSilent())
            Logger.log(
                    Logger.INFO,
                    EtlLogger.class,
                    EtlResource.PERSISTING_DATASET_MSG.getValue()
                            + dataSet.getName() + "...");
        
        try
        {
            prePersist(params, dataSet, driver);
            
            int records = dataSet.getRecordCount();
            
            for (int row = 0; row < records; row++)
            {
                DataSetRecord record = dataSet.getRecord(row);
                
                inlinePersist(params, dataSet, driver, record, row, records);
            }
            
            postPersist(params, dataSet, driver);
        }
        finally
        {
            cleanUp(params, dataSet, driver);
        }
        
        ConnectorResult connectorResult = new ConnectorResult();
        
        connectorResult
                .addResult(Utils.format(FileConnectorResource.FILE_PERSISTED
                        .getValue(), new String[] {FilenameUtils.getName(params
                        .getRealFileName())}));
        
        return connectorResult;
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#populate(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult populate(TextConnectorParams params,
            DataSet dataSet, Driver driver)
        throws Exception
    {
        if (params == null || dataSet == null
                || Utils.isNothing(dataSet.getName())
                || (driver == null && dataSet.getDriver() == null))
        {
            ConnectorResult result = new ConnectorResult();
            result.setRetCode(ConnectorResult.VALIDATION_FAILED_CODE);
            
            if (dataSet == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NULL
                        .getValue());
            if (driver == null && dataSet.getDriver() == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DRIVER_NULL
                        .getValue());
            if (params == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_PARAMS_NULL
                        .getValue());
            if (dataSet != null && Utils.isNothing(dataSet.getName()))
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NO_NAME
                        .getValue());
            
            return result;
        }
        
        dataSet.clear();
        
        String delimiter = Utils.str2Regexp(params.getDelimiter());
        
        driver = driver != null ? driver : dataSet.getDriver();
        
        if (!params.isSilent())
            Logger.log(
                    Logger.INFO,
                    EtlLogger.class,
                    EtlResource.LOADING_DATASET_MSG.getValue()
                            + dataSet.getName() + "...");
        
        try
        {
            Map<Integer, Boolean> types = new HashMap<Integer, Boolean>();
            
            boolean hasMetadata = false;
            
            if (params.isPersistMetaData() && params.getInputStream() == null)
            {
                File file = new File(
                        SystemConfig
                                .instance()
                                .getPathUsingAppFolders(
                                        params.getFileName(
                                                dataSet.getOwnerName() != null ? dataSet
                                                        .getOwnerName()
                                                        : dataSet.getName(),
                                                ".xml", true)));
                
                hasMetadata = file.exists();
            }
            
            if (hasMetadata)
                getXmlConnector().populate(getXmlParams(params), dataSet,
                        driver);
            
            if (params.getBeforeCallback() != null)
                params.getBeforeCallback().onBefore(dataSet, driver);
            
            String fileName = null;
            
            File dataFile = null;
            
            if (params.getInputStream() == null)
            {
                fileName = SystemConfig.instance().getPathUsingAppFolders(
                        params.getFileName(
                                dataSet.getOwnerName() != null ? dataSet
                                        .getOwnerName() : dataSet.getName(),
                                params.getExt(), false));
                
                dataFile = new File(fileName);
                
                if (!dataFile.exists())
                {
                    ConnectorResult result = new ConnectorResult();
                    result.setRetCode(ConnectorResult.VALIDATION_FAILED_CODE);
                    
                    result.addResult(FileConnectorResource.VALIDATION_ERROR_FILE_DOESN_EXIST
                            .getValue());
                    
                    return result;
                }
            }
            
            int[] lengthArray = getLengthArray(params, -1);
            
            boolean hasKey = !Utils.isNothing(dataSet.getKeyFields());
            
            if (dataSet.getData() == null)
                dataSet.setData(new DataSetData());
            
            int index = 1;
            
            // load data
            BufferedReader input = null;
            try
            {
                if (params.getInputStream() == null)
                    input = new BufferedReader(new FileReader(dataFile));
                else
                    input = new BufferedReader(new InputStreamReader(
                            params.getInputStream()));
                
                String line = null;
                try
                {
                    while ((line = input.readLine()) != null)
                    {
                        parseLine(dataSet, driver, line, params, delimiter,
                                index++, hasKey, hasMetadata, types,
                                lengthArray);
                    }
                }
                catch (Exception ex)
                {
                    if (!params.isMaxRowsExceededException(ex))
                        throw ex;
                }
                
                if (dataSet.getFieldCount() > 0
                        && dataSet.getRecordCount() == 0
                        && params.getAddRecordCallback() != null)
                {
                    params.getAddRecordCallback().onAddRecord(dataSet, driver,
                            null, 0);
                }
                
                ConnectorResult connectorResult = new ConnectorResult();
                
                connectorResult.addResult(Utils.format(
                        FileConnectorResource.FILE_POPULATED.getValue(),
                        new String[] {FilenameUtils.getName(fileName)}));
                
                return connectorResult;
            }
            finally
            {
                if (input != null)
                    try
                    {
                        input.close();
                    }
                    catch (IOException ex)
                    {
                        Logger.log(Logger.INFO, this,
                                Resource.ERROR_GENERAL.getValue(), ex);
                        
                    }
                
                if (params.getInputStream() != null && params.isCloseInput())
                    try
                    {
                        params.getInputStream().close();
                    }
                    catch (IOException ex)
                    {
                        Logger.log(Logger.INFO, this,
                                Resource.ERROR_GENERAL.getValue(), ex);
                        
                    }
            }
        }
        finally
        {
            if (params.getAfterCallback() != null)
                params.getAfterCallback().onAfter(dataSet, driver);
            
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#postPersist(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public void postPersist(TextConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        params.setPostPersistOccured(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#prePersist(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public void prePersist(TextConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        if (params.isPersistMetaData() && params.getInputStream() == null)
            writeMetaData(params, dataSet, driver);
        
        int[] lengthArray = getLengthArray(params, dataSet.getFieldCount());
        
        params.setLengthArray(lengthArray);
        
        Writer output = null;
        
        if (params.getOutputStream() == null)
        {
            
            String fileName = SystemConfig.instance().getPathUsingAppFolders(
                    params.getFileName(
                            dataSet.getOwnerName() != null ? dataSet
                                    .getOwnerName() : dataSet.getName(), params
                                    .getExt(), false));
            
            params.setRealFileName(fileName);
            
            File file = new File(fileName);
            
            output = new BufferedWriter(new FileWriter(file));
            
            if (params.getTransactionMonitor() != null)
                params.getTransactionMonitor().addFile(fileName);
        }
        else
            output = new BufferedWriter(new OutputStreamWriter(
                    params.getOutputStream()));
        
        params.setWriter(output);
        
        if (!params.isFirstRowData())
        {
            String titleRecordS = "";
            
            for (int col = 0; col < dataSet.getFieldCount(); col++)
            {
                FieldDef fieldDef = dataSet.getFields().get(col);
                
                if (!fieldDef.isVisible())
                    continue;
                
                if (lengthArray == null)
                {
                    if (dataSet.getFieldCount() > 1 && col > 0)
                        titleRecordS = titleRecordS + params.getDelimiter()
                                + fieldDef.getName();
                    else
                        titleRecordS = titleRecordS + fieldDef.getName();
                }
                else
                {
                    titleRecordS = titleRecordS
                            + Utils.padRight(fieldDef.getName(),
                                    lengthArray[col], true);
                }
            }
            
            output.write(titleRecordS.replaceAll("(\\r|\\n)", ""));
            output.write(Utils.NEWLINE);
        }
        
        params.setPrePersistOccured(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#writeMetaData(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult writeMetaData(TextConnectorParams params,
            DataSet dataSet, Driver driver)
        throws Exception
    {
        return getXmlConnector().writeMetaData(getXmlParams(params), dataSet,
                driver);
    }
    
}
