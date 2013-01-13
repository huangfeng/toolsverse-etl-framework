/*
 * SqlConnector.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.sql;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetData;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.connector.BaseDataSetConnector;
import com.toolsverse.etl.connector.ConnectorResource;
import com.toolsverse.etl.connector.ConnectorResult;
import com.toolsverse.etl.connector.DataSetConnector;
import com.toolsverse.etl.connector.FileConnectorResource;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.connection.AliasConnectionProvider;
import com.toolsverse.etl.sql.connection.ConnectionProvider;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * Reads data from the database using jdbc, creates insert sql statements. Supports data streaming.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class SqlConnector extends BaseDataSetConnector implements
        DataSetConnector<SqlConnectorParams, ConnectorResult>
{
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#cleanUp(com.toolsverse.
     * etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public void cleanUp(SqlConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        if (params.getWriter() != null)
            params.getWriter().close();
        
        params.setCleanUpPersistOccurred(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#getDataSetConnectorParams()
     */
    public SqlConnectorParams getDataSetConnectorParams()
    {
        return new SqlConnectorParams();
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
     * Gets the fields and values for the given record.
     *
     * @param dataSet the data set
     * @param driver the driver
     * @param record the record
     * @param row the row
     * @return the list of fields and list of values
     * @throws Exception in case of any error
     */
    private TypedKeyValue<List<String>, List<String>> getFieldsAndValues(
            DataSet dataSet, Driver driver, DataSetRecord record, int row)
        throws Exception
    {
        List<String> fields = new ArrayList<String>();
        
        List<String> values = new ArrayList<String>();
        
        int count = dataSet.getFieldCount();
        
        for (int i = 0; i < count; i++)
        {
            FieldDef fieldDef = dataSet.getFieldDef(i);
            
            if (!fieldDef.isVisible())
                continue;
            
            fields.add(fieldDef.getName());
            
            values.add(driver.convertValueForStorage(
                    dataSet.getFieldValue(row, i), fieldDef.getSqlDataType(),
                    false));
        }
        
        return new TypedKeyValue<List<String>, List<String>>(fields, values);
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
        return "SQL";
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
        return "com/toolsverse/etl/connector/sql/sql_config.xml";
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
    public void inlinePersist(SqlConnectorParams params, DataSet dataSet,
            Driver driver, DataSetRecord record, int row, int records)
        throws Exception
    {
        if (record == null)
            return;
        
        TypedKeyValue<List<String>, List<String>> fieldsAndValues = getFieldsAndValues(
                dataSet, driver, record, row);
        
        String insertSql = driver.getInsertStatement(fieldsAndValues,
                SqlUtils.name2RightCase(driver, params.getTableName()));
        
        if (row >= 0 && records >= 0 && !params.isSilent()
                && params.getLogStep() > 0 && (row % params.getLogStep()) == 0)
            Logger.log(Logger.INFO, EtlLogger.class, dataSet.getName() + ": "
                    + EtlResource.PERSITING_RECORD.getValue() + row
                    + " out of " + records);
        
        params.getWriter().write(insertSql);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#persist(com.toolsverse.
     * etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult persist(SqlConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        if (dataSet == null
                || params == null
                || (driver == null && dataSet.getDriver() == null)
                || Utils.isNothing(dataSet.getName())
                || dataSet.getFieldCount() == 0
                || dataSet.getRecordCount() == 0
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
            if (dataSet != null && dataSet.getRecordCount() == 0)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NO_DATA
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
        
        if (!Utils.isNothing(params.getTableName()))
        {
            dataSet.setTableName(params.getTableName());
        }
        
        driver = driver != null ? driver : dataSet.getDriver();
        
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
                inlinePersist(params, dataSet, driver, dataSet.getRecord(row),
                        row, records);
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
    public ConnectorResult populate(SqlConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        if (params == null || dataSet == null
                || (driver == null && dataSet.getDriver() == null)
                || params.getResultSet() == null)
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
            
            return result;
        }
        
        dataSet.clear();
        
        driver = driver != null ? driver : dataSet.getDriver();
        
        ResultSet rs = params.getResultSet();
        
        if (!params.isSilent())
            Logger.log(
                    Logger.INFO,
                    EtlLogger.class,
                    EtlResource.POPULATING_DATASET_MSG.getValue()
                            + dataSet.getName() + "...");
        
        try
        {
            ResultSetMetaData metaData = rs.getMetaData();
            
            if (metaData == null || metaData.getColumnCount() == 0)
            {
                ConnectorResult result = new ConnectorResult();
                
                result.setRetCode(ConnectorResult.VALIDATION_FAILED_CODE);
                
                result.addResult(SqlConnectorResource.VALIDATION_ERROR_NO_METADATA_FOUND
                        .getValue());
                
                return result;
            }
            
            ListHashMap<String, FieldDef> fields = null;
            if (params.getFieldsMapping() != null
                    && params.getFieldsMapping().size() > 0)
                fields = new ListHashMap<String, FieldDef>();
            
            ListHashMap<String, FieldDef> allFields = new ListHashMap<String, FieldDef>();
            DataSetData data = new DataSetData();
            Map<String, Integer> cols = new HashMap<String, Integer>();
            
            FieldDef fieldDef = null;
            String colName;
            Object colValue;
            String dataSize;
            DataSetRecord record;
            int colCount = metaData.getColumnCount();
            
            Map<String, Object> values = null;
            if (params.isUnique())
                values = new HashMap<String, Object>();
            
            for (int col = 1; col <= colCount; col++)
            {
                colName = SqlUtils.getColName(cols, metaData.getColumnName(col)
                        .toUpperCase());
                
                dataSize = "";
                int precision;
                int scale;
                
                try
                {
                    precision = metaData.getPrecision(col);
                }
                // cannot get precision. It's ok
                catch (Exception ex)
                {
                    precision = -1;
                }
                
                int fieldSize = -1;
                
                if (precision > 0)
                {
                    fieldSize = precision;
                }
                else
                {
                    fieldSize = metaData.getColumnDisplaySize(col);
                    
                    if (fieldSize > 0)
                        precision = fieldSize;
                }
                
                try
                {
                    scale = metaData.getScale(col);
                }
                // cannot get scale It's ok
                catch (Exception ex)
                {
                    scale = -1;
                }
                
                if (scale != -1 && scale == driver.getWrongScale())
                {
                    scale = -1;
                }
                
                if (precision > 0 && scale <= 0)
                    dataSize = "(" + precision + ")";
                else if (precision > 0 && scale > 0)
                    dataSize = "(" + precision + "," + scale + ")";
                
                fieldDef = new FieldDef();
                fieldDef.setName(colName);
                fieldDef.setSqlDataType(metaData.getColumnType(col));
                fieldDef.setNativeDataType(metaData.getColumnTypeName(col)
                        + dataSize);
                fieldDef.setDataSize(dataSize);
                fieldDef.setPrecision(precision);
                fieldDef.setScale(scale);
                fieldDef.setNullable(metaData.isNullable(col) != ResultSetMetaData.columnNoNulls);
                fieldDef.setFieldSize(fieldSize);
                fieldDef.setAutoIncrement(metaData.isAutoIncrement(col));
                
                allFields.put(colName, fieldDef);
                
                // remap fields
                if (fields != null)
                {
                    String newName = params.getFieldsMapping().get(colName);
                    
                    if (Utils.isNothing(newName))
                        continue;
                    
                    newName = SqlUtils.getValue(newName, 0, ";");
                    
                    FieldDef newFieldDef = new FieldDef();
                    newFieldDef.setName(newName);
                    newFieldDef.setSqlDataType(fieldDef.getSqlDataType());
                    newFieldDef.setNativeDataType(fieldDef.getNativeDataType());
                    newFieldDef.setDataSize(fieldDef.getDataSize());
                    newFieldDef.setPrecision(fieldDef.getPrecision());
                    newFieldDef.setScale(fieldDef.getScale());
                    newFieldDef.setNullable(fieldDef.isNullable());
                    newFieldDef.setFieldSize(fieldDef.getFieldSize());
                    newFieldDef.setAutoIncrement(fieldDef.isAutoIncrement());
                    
                    fields.put(newName, newFieldDef);
                }
            }
            
            Object[] rec = null;
            
            // rearrange fields
            if (fields != null && fields.size() > 0)
            {
                List<FieldDef> list = fields.getList();
                list.clear();
                
                List<String> mapList = params.getFieldsMapping().getList();
                for (int ind = 0; ind < mapList.size(); ind++)
                {
                    String cName = SqlUtils.getValue(mapList.get(ind), 0, ";");
                    
                    FieldDef fieldDefToAdd = fields.get(cName);
                    
                    if (fieldDefToAdd != null)
                        list.add(fieldDefToAdd);
                }
                
                rec = new Object[fields.size()];
            }
            
            dataSet.setFields(allFields);
            dataSet.setData(data);
            
            // before something
            if (params.getBeforeCallback() != null)
                params.getBeforeCallback().onBefore(dataSet, driver);
            
            int index = 1;
            
            while (rs.next())
            {
                if (!params.isSilent() && params.getLogStep() > 0
                        && (index % params.getLogStep()) == 0)
                    Logger.log(
                            Logger.INFO,
                            EtlLogger.class,
                            dataSet.getName()
                                    + ": "
                                    + index
                                    + EtlResource.READING_DATASET_MSG
                                            .getValue());
                index++;
                
                record = new DataSetRecord();
                boolean skip = false;
                String value = "";
                
                for (int col = 0; col < colCount; col++)
                {
                    fieldDef = allFields.get(col);
                    
                    String fName = fieldDef.getName();
                    
                    if (params.getFieldsMapping() != null)
                        fName = SqlUtils.getValue(params.getFieldsMapping()
                                .get(fieldDef.getName()), 0, ";");
                    
                    if (fields == null || fields.size() == 0
                            || fields.containsKey(fName))
                    {
                        colValue = driver.getObject(rs, col,
                                fieldDef.getSqlDataType());
                        
                        if (rec != null)
                        {
                            if (colValue == null)
                            {
                                String defValue = SqlUtils.getValue(
                                        params.getFieldsMapping().get(
                                                fieldDef.getName()), 1, ";");
                                
                                if (!Utils.isNothing(defValue))
                                    colValue = defValue;
                            }
                            
                            int recIndex = fields.getList().indexOf(
                                    fields.get(fName));
                            
                            if (recIndex >= 0 && recIndex < rec.length)
                                rec[recIndex] = colValue;
                        }
                        else
                        {
                            if (params.getAddFieldValueCallback() != null)
                                params.getAddFieldValueCallback()
                                        .onAddFieldValue(dataSet, driver,
                                                record, fieldDef);
                            
                            addValue(colValue, record, dataSet);
                        }
                        
                        if ((params.isCheckKeyField()
                                && fName.equalsIgnoreCase(dataSet
                                        .getKeyFields()) && Utils
                                    .isNothing(colValue))
                                || (params.getFilterByField() != null
                                        && params.getFilterByFieldValue() != null
                                        && fName.equalsIgnoreCase(params
                                                .getFilterByField()) && !params
                                        .getFilterByFieldValue().equals(
                                                colValue)))
                        {
                            skip = true;
                            break;
                        }
                        
                        if (params.isUnique())
                            value = value + (colValue != null ? colValue : "");
                    }
                }
                
                if (skip)
                    continue;
                
                if (params.isUnique())
                {
                    if (values.containsKey(value))
                        continue;
                    
                    values.put(value, null);
                }
                
                // rearrange fields in the record
                if (rec != null)
                    for (int ind = 0; ind < rec.length; ind++)
                        addValue(rec[ind], record, dataSet);
                
                if (params.getMaxRows() >= 0
                        && data.size() >= params.getMaxRows())
                {
                    break;
                }
                
                boolean added = dataSet.addRecord(record);
                
                // inline something
                if (added && params.getAddRecordCallback() != null)
                    params.getAddRecordCallback().onAddRecord(dataSet, driver,
                            record, index - 1);
            }
            
            if (fields != null && fields.size() > 0)
                dataSet.setFields(fields);
            
            if (dataSet.getFieldCount() > 0 && dataSet.getRecordCount() == 0
                    && params.getAddRecordCallback() != null)
            {
                params.getAddRecordCallback().onAddRecord(dataSet, driver,
                        null, 0);
            }
        }
        finally
        {
            SqlUtils.cleanUpSQLData(null, rs, this);
            
            if (params.getAfterCallback() != null)
                params.getAfterCallback().onAfter(dataSet, driver);
        }
        
        return new ConnectorResult();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#postPersist(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public void postPersist(SqlConnectorParams params, DataSet dataSet,
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
    public void prePersist(SqlConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        String fileName = SystemConfig.instance().getPathUsingAppFolders(
                params.getFileName(dataSet.getName()));
        
        File file = new File(fileName);
        
        String tableName = !Utils.isNothing(dataSet.getTableName()) ? dataSet
                .getTableName() : dataSet.getName();
        
        Writer output = new BufferedWriter(new FileWriter(file));
        
        if (params.getTransactionMonitor() != null)
            params.getTransactionMonitor().addFile(fileName);
        
        params.setRealFileName(fileName);
        params.setTableName(tableName);
        params.setWriter(output);
        
        params.setPrePersistOccured(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#testConnection(com.toolsverse
     * .etl.common.ConnectionParams)
     */
    @Override
    public String testConnection(ConnectionParams connectionParams)
        throws Exception
    {
        if (!(connectionParams instanceof Alias))
        {
            return ConnectorResource.UNKNOWN_CONNECTION_TYPE.getValue();
        }
        
        if (!connectionParams.isDbConnection())
        {
            return ConnectorResource.NOT_A_DB_CONNECTION.getValue();
        }
        
        @SuppressWarnings("unchecked")
        ConnectionProvider<Alias> connectionProvider = (ConnectionProvider<Alias>)ObjectFactory
                .instance().get(ConnectionProvider.class.getName(),
                        AliasConnectionProvider.class.getName(), true);
        Connection con = null;
        
        try
        {
            con = connectionProvider.getConnection((Alias)connectionParams);
            
            return "";
        }
        finally
        {
            if (connectionProvider != null)
                connectionProvider.releaseConnection(con);
        }
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#writeMetaData(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult writeMetaData(SqlConnectorParams params,
            DataSet dataSet, Driver driver)
        throws Exception
    {
        return new ConnectorResult();
    }
    
}
