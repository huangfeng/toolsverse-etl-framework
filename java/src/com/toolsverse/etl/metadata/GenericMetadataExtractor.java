/*
 * GenericMetadataExtractor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.metadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * The default implementation of the MetadataExtractor for the jdbc compliant databases.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class GenericMetadataExtractor implements MetadataExtractor
{
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.MetadataExtractor#getMetaData(java.lang
     * .String, java.sql.Connection, com.toolsverse.etl.driver.Driver,
     * java.lang.String, boolean, boolean)
     */
    public Map<String, FieldDef> getMetaData(String objectName,
            Connection connection, Driver driver, String sql, boolean useTypes,
            boolean keepOrder)
    {
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        
        Map<String, FieldDef> metadaData = null;
        
        if (!keepOrder)
            metadaData = new TreeMap<String, FieldDef>(
                    String.CASE_INSENSITIVE_ORDER);
        else
            metadaData = new LinkedHashMap<String, FieldDef>();
        
        if ((Utils.isNothing(objectName) && Utils.isNothing(sql))
                || connection == null)
            return metadaData;
        
        String stSql = SqlUtils.getSqlForMetadataExtraction(
                Utils.isNothing(sql) ? objectName : sql, driver);
        
        try
        {
            preparedStatement = connection.prepareStatement(stSql);
            preparedStatement.setMaxRows(1);
            preparedStatement.setFetchSize(1);
            
            ResultSetMetaData metaData = null;
            
            Savepoint svp = null;
            
            try
            {
                svp = driver.requiresRollbackAfterSqlError() ? SqlUtils
                        .getSavepoint(connection) : null;
                
                metaData = preparedStatement.getMetaData();
                
                if (metaData == null)
                    throw new SQLException("no metadata");
            }
            catch (Exception ex)
            {
                if (svp != null)
                    connection.rollback(svp);
                
                svp = driver.requiresRollbackAfterSqlError() ? SqlUtils
                        .getSavepoint(connection) : null;
                
                try
                {
                    rs = preparedStatement.executeQuery();
                    
                    metaData = rs.getMetaData();
                }
                finally
                {
                    if (svp != null)
                        connection.rollback(svp);
                }
            }
            
            parseResultSetMetaData(metaData, metadaData, useTypes, driver);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.WARNING, EtlLogger.class,
                    EtlResource.ERROR_GETTING_METADATA.getValue(), ex);
            metadaData = null;
        }
        finally
        {
            SqlUtils.cleanUpSQLData(preparedStatement, rs, this);
        }
        
        return metadaData;
        
    }
    
    /**
     * Parses the result set meta data.
     *
     * @param metaData the meta data
     * @param fields the fields
     * @param useTypes the "use types" flag. If true will use exact data types. Useful for automatic table creation.
     * @param driver the driver
     * @throws Exception in case of any error
     */
    private void parseResultSetMetaData(ResultSetMetaData metaData,
            Map<String, FieldDef> fields, boolean useTypes, Driver driver)
        throws Exception
    {
        int colCount = metaData.getColumnCount();
        for (int i = 1; i <= colCount; i++)
        {
            String fldName = metaData.getColumnName(i).toUpperCase();
            
            FieldDef fieldDef = new FieldDef();
            
            fieldDef.setName(fldName);
            
            if (!Utils.isNothing(fldName))
            {
                if (useTypes)
                {
                    String dataSize = "";
                    
                    int fType = metaData.getColumnType(i);
                    
                    boolean isNotNullbale = metaData.isNullable(i) == ResultSetMetaData.columnNoNulls;
                    
                    if (SqlUtils.dataTypeHasSize(fType))
                    {
                        
                        int precision;
                        int scale;
                        
                        try
                        {
                            precision = metaData.getPrecision(i);
                        }
                        // cannot get precision. It's ok
                        catch (Exception ex)
                        {
                            precision = -1;
                        }
                        
                        if (precision > 0)
                        {
                            fieldDef.setPrecision(precision);
                            fieldDef.setFieldSize(precision);
                        }
                        else
                        {
                            int fieldSize = metaData.getColumnDisplaySize(i);
                            fieldDef.setFieldSize(fieldSize);
                            
                            if (fieldSize > 0)
                            {
                                precision = fieldSize;
                                
                                fieldDef.setPrecision(precision);
                            }
                        }
                        
                        try
                        {
                            scale = metaData.getScale(i);
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
                        
                        if (scale > 0)
                            fieldDef.setScale(scale);
                        
                        if (precision > 0 && scale <= 0)
                            dataSize = "(" + precision + ")";
                        else if (precision > 0 && scale > 0)
                            dataSize = "(" + precision + "," + scale + ")";
                    }
                    
                    String nativeType = metaData.getColumnTypeName(i)
                            + dataSize;
                    
                    fieldDef.setNullable(!isNotNullbale);
                    fieldDef.setSqlDataType(fType);
                    fieldDef.setNativeDataType(nativeType);
                    fieldDef.setAutoIncrement(metaData.isAutoIncrement(i));
                    
                }
                
                fieldDef.setIndex(i - 1);
                
                fields.put(fldName, fieldDef);
            }
        }
    }
    
}
