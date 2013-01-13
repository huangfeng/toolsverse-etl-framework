/*
 * ExcelDriver.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.driver.excel;

import java.sql.Connection;
import java.sql.Types;
import java.util.Date;
import java.util.Map;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.FieldsRepository;
import com.toolsverse.etl.driver.SimpleDriver;
import com.toolsverse.etl.parser.SqlParser;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.UrlUtils;
import com.toolsverse.util.Utils;

/**
 * Excel odbc ETL driver.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.5
 */

public class ExcelOdbcDriver extends SimpleDriver
{
    
    /** The jdbc driver class name. */
    private static final String JDBC_DRIVER_CLASS = "sun.jdbc.odbc.JdbcOdbcDriver";
    
    /** The url. */
    private static final String JDBC_URL = "jdbc:odbc:Driver={Microsoft Excel Driver (*.xls)};READONLY=false;DBQ=<file>"
            + UrlUtils.getUrlToken(UrlUtils.FILE_TOKEN);
    
    /** The metadata driver class name. */
    private static final String METADATA_CLASS = "com.toolsverse.etl.metadata.excel.ExcelOdbcMetadata";
    
    /** The EXCEL TIMESTAMP FORMAT. */
    public static final String EXCEL_TIMESTAMP_FORMAT = "MM/dd/yyyy HH:mm:ss";
    
    /** The EXCEL DATE FORMAT. */
    public static final String EXCEL_DATE_FORMAT = "MM/dd/yyyy";
    
    /** The EXCEL TIME FORMAT. */
    public static final String EXCEL_TIME_FORMAT = "HH:mm:ss";
    
    /** The maximum string literal size. */
    public static int MAX_STRING_LITERAL_SIZE = 255;
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.SimpleDriver#convertValueForStorage(java.lang
     * .Object, int, boolean)
     */
    @Override
    public String convertValueForStorage(Object fieldValue, int fieldType,
            boolean isFromTable)
    {
        if (fieldValue == null)
            return "NULL";
        
        String value = filter(fieldValue.toString());
        
        if (SqlUtils.isChar(fieldType) || SqlUtils.isClob(fieldType))
        {
            return convertStringForStorage(value);
        }
        
        switch (fieldType)
        {
            case Types.DATE:
                if (fieldValue instanceof Date)
                    return
                    
                    "'" + Utils.date2Str((Date)fieldValue, EXCEL_DATE_FORMAT)
                            + "'";
                else
                    return "'" + value + "'";
            case Types.TIME:
                if (fieldValue instanceof Date)
                    return "'"
                            + Utils.date2Str((Date)fieldValue,
                                    EXCEL_TIME_FORMAT) + "'";
                else
                    return "'" + value + "'";
            case Types.TIMESTAMP:
                if (fieldValue instanceof Date)
                    return "'"
                            + Utils.date2Str((Date)fieldValue,
                                    EXCEL_TIMESTAMP_FORMAT) + "'";
                else
                    return "'" + value + "'";
            default:
                return value;
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getCmdForExternalTool(com.toolsverse
     * .util.ObjectStorage, com.toolsverse.etl.common.Alias, java.lang.String)
     */
    public String getCmdForExternalTool(ObjectStorage storage, Alias alias,
            String sqlFile)
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getCreateTableSql(java.lang.String,
     * com.toolsverse.etl.common.DataSet, boolean, java.lang.String,
     * com.toolsverse.etl.common.FieldsRepository)
     */
    @Override
    public String getCreateTableSql(String name, DataSet dataSet,
            boolean isTemporary, String key, FieldsRepository fieldsRepository)
    {
        name = name.replaceAll("\\[", "").replaceAll("\\]", "")
                .replaceAll("\\$", "");
        
        return "create table " + name + " ("
                + SqlUtils.getFieldsSql(dataSet, this, key, fieldsRepository)
                + ");";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getExplainPlan(com.toolsverse.util.
     * ObjectStorage, java.sql.Connection, com.toolsverse.etl.common.Alias,
     * java.lang.String)
     */
    public Object getExplainPlan(ObjectStorage storage, Connection connection,
            Alias alias, String sql)
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getExternalToolName()
     */
    public String getExternalToolName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getIconPath()
     */
    public String getIconPath()
    {
        return "com/toolsverse/etl/driver/excel/images/excel_logo.gif";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getJdbcDriverClassName()
     */
    public String getJdbcDriverClassName()
    {
        return JDBC_DRIVER_CLASS;
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
     * @see com.toolsverse.etl.driver.SimpleDriver#getMaxStringLiteralSize()
     */
    @Override
    public int getMaxStringLiteralSize()
    {
        int size = super.getMaxStringLiteralSize();
        
        if (size >= 0)
            return size;
        else
            return MAX_STRING_LITERAL_SIZE;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getMetadataClassName()
     */
    public String getMetadataClassName()
    {
        return METADATA_CLASS;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getName()
     */
    public String getName()
    {
        return "Excel ODBC";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getSqlForExternalTool(com.toolsverse
     * .etl.common.Alias, java.lang.String, com.toolsverse.etl.parser.SqlParser)
     */
    public String getSqlForExternalTool(Alias alias, String sql,
            SqlParser parser)
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.AbstractDriver#getTableName(java.lang.String)
     */
    @Override
    public String getTableName(String name)
    {
        name = SqlUtils.baseName2Name(name);
        
        if (name.lastIndexOf('$') == name.length() - 1)
            return "[" + name + "]";
        else
            return "[" + name + "$]";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getUrlPattern()
     */
    public String getUrlPattern()
    {
        return JDBC_URL;
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
     * @see com.toolsverse.ext.ExtensionModule#getXmlConfigFileName()
     */
    public String getXmlConfigFileName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsNotNullable()
     */
    @Override
    public boolean supportsNotNullable()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.AbstractDriver#supportsParallelExtract()
     */
    @Override
    public boolean supportsParallelExtract()
    {
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.AbstractDriver#supportsParallelLoad()
     */
    @Override
    public boolean supportsParallelLoad()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsScripts()
     */
    public boolean supportsScripts()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.AbstractDriver#tableName2Name(java.lang.String)
     */
    @Override
    public String tableName2Name(String name)
    {
        return name.replaceAll("\\[", "").replaceAll("\\]", "")
                .replaceAll("\\$", "");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.SimpleDriver#value2StorageValue(int,
     * java.lang.Object, java.util.Map)
     */
    @Override
    public Object value2StorageValue(int fieldType, Object value,
            Map<String, String> params)
    {
        if (value == null)
            return null;
        else
        {
            switch (fieldType)
            {
                case Types.DATE:
                    return Utils.date2Str((Date)value, EXCEL_DATE_FORMAT);
                case Types.TIME:
                    return Utils.date2Str((Date)value, EXCEL_TIME_FORMAT);
                case Types.TIMESTAMP:
                    return Utils.date2Str((Date)value, EXCEL_TIMESTAMP_FORMAT);
            }
            
            return value.toString();
        }
    }
    
}
