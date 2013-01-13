/*
 * CallableDriver.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.driver;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * The default abstract implementation of the Driver interface which supports callable statements. 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public abstract class CallableDriver extends AbstractDriver
{
    
    /** The override max string literal size. */
    private int _overrideMaxStringLiteralSize = -1;
    
    /** The override max varchar size. */
    private int _overrideMaxVarcharSize = -1;
    
    /** The override max char size. */
    private int _overrideMaxCharSize = -1;
    
    /** The override lines limit. */
    private int _overrideLinesLimit = -1;
    
    /** The override max precision. */
    private int _overrideMaxPrecision = -1;
    
    /** The _override max scale. */
    private int _overrideMaxScale = -1;
    
    /** The init sql. */
    private String _initSql = null;
    
    /**
     * Gets init sql.
     *
     * @return the string
     */
    protected abstract String _getInitSql();
    
    /**
     * Gets lines limit.
     *
     * @return the lines limit
     */
    protected abstract int _getLinesLimit();
    
    /**
     * Gets max char size.
     *
     * @return the max char size
     */
    protected abstract int _getMaxCharSize();
    
    /**
     * Gets max precision.
     *
     * @return the max precision
     */
    protected abstract int _getMaxPrecision();
    
    /**
     * Gets the max scale.
     *
     * @return the max scale
     */
    protected abstract int _getMaxScale();
    
    /**
     * Gets the max string literal size.
     *
     * @return the max string literal size
     */
    protected abstract int _getMaxStringLiteralSize();
    
    /**
     * Gets the max varchar size.
     *
     * @return the max varchar size
     */
    protected abstract int _getMaxVarcharSize();
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#deleteStagingBinary(java.sql.Connection,
     * java.lang.String)
     */
    public void deleteStagingBinary(Connection conn, String tableName)
        throws Exception
    {
        String sql = "delete from " + getTempTableName("external_blobs")
                + " where table_name = ?";
        
        PreparedStatement statement = null;
        
        try
        {
            statement = conn.prepareStatement(sql);
            statement.setString(1, tableName);
            
            statement.executeUpdate();
        }
        finally
        {
            SqlUtils.cleanUpSQLData(statement, null, this);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getBlob(java.sql.ResultSet, int)
     */
    public Object getBlob(ResultSet rs, int pos)
        throws Exception
    {
        return SqlUtils.getBlob(rs, pos);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getClob(java.sql.ResultSet, int)
     */
    public Object getClob(ResultSet rs, int pos)
        throws Exception
    {
        return SqlUtils.getClob(rs, pos);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getDestinationInfo(java.lang.String)
     */
    public TypedKeyValue<String, Integer> getDestinationInfo(String sql)
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getInitSql()
     */
    public String getInitSql()
    {
        if (_initSql != null)
            return _getInitSql() + _initSql;
        else
            return _getInitSql();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getLinesLimit()
     */
    public int getLinesLimit()
    {
        if (_overrideLinesLimit >= 0)
            return _overrideLinesLimit;
        else
            return _getLinesLimit();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getMaxCharSize()
     */
    public int getMaxCharSize()
    {
        if (_overrideMaxCharSize >= 0)
            return _overrideMaxCharSize;
        else
            return _getMaxCharSize();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getMaxPrecision()
     */
    public int getMaxPrecision()
    {
        if (_overrideMaxPrecision >= 0)
            return _overrideMaxPrecision;
        else
            return _getMaxPrecision();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getMaxScale()
     */
    public int getMaxScale()
    {
        if (_overrideMaxScale >= 0)
            return _overrideMaxScale;
        else
            return _getMaxScale();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getMaxStringLiteralSize()
     */
    public int getMaxStringLiteralSize()
    {
        if (_overrideMaxStringLiteralSize >= 0)
            return _overrideMaxStringLiteralSize;
        else
            return _getMaxStringLiteralSize();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getMaxVarcharSize()
     */
    public int getMaxVarcharSize()
    {
        if (_overrideMaxVarcharSize >= 0)
            return _overrideMaxVarcharSize;
        else
            return _getMaxVarcharSize();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getObject(java.sql.ResultSet, int,
     * int)
     */
    public Object getObject(ResultSet rs, int index, int fieldType)
        throws Exception
    {
        try
        {
            
            if (SqlUtils.isClob(fieldType))
                return getClob(rs, index + 1);
            else if (SqlUtils.isBlob(fieldType))
                return getBlob(rs, index + 1);
            else
                return rs.getObject(index + 1);
        }
        catch (Exception ex)
        {
            // will try to recover
            return rs.getObject(index + 1);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getParentDriverName()
     */
    public String getParentDriverName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getVarName(java.lang.String)
     */
    public String getVarName(String name)
    {
        if (name == null)
            return null;
        
        return "v" + name;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getWrongPrecision()
     */
    public int getWrongScale()
    {
        return -1;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#replaceOnException(java.lang.String)
     */
    public String replaceOnException(String sql)
    {
        return sql;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setBlob(java.sql.PreparedStatement,
     * java.lang.Object, int)
     */
    public void setBlob(PreparedStatement pstmt, Object value, int pos)
        throws Exception
    
    {
        SqlUtils.setBlob(pstmt, value, pos);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setClob(java.sql.PreparedStatement,
     * java.lang.Object, int)
     */
    public void setClob(PreparedStatement pstmt, Object value, int pos)
        throws Exception
    
    {
        SqlUtils.setClob(pstmt, value, pos);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setInitSql(java.lang.String)
     */
    public void setInitSql(String value)
    {
        _initSql = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setLinesLimit(int)
     */
    public void setLinesLimit(int value)
    {
        _overrideLinesLimit = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setMaxCharSize(int)
     */
    public void setMaxCharSize(int value)
    {
        _overrideMaxCharSize = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setMaxPrecision(int)
     */
    public void setMaxPrecision(int value)
    {
        _overrideMaxPrecision = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setMaxScale(int)
     */
    public void setMaxScale(int value)
    {
        _overrideMaxScale = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setMaxStringLiteralSize(int)
     */
    public void setMaxStringLiteralSize(int value)
    {
        _overrideMaxStringLiteralSize = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setMaxVarcharSize(int)
     */
    public void setMaxVarcharSize(int value)
    {
        _overrideMaxVarcharSize = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#setParentDriverName(java.lang.String)
     */
    public void setParentDriverName(String value)
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsScripts()
     */
    public boolean supportsScripts()
    {
        return true;
    }
    
    /**
     * Updates staging binary.
     *
     * @param conn the connection
     * @param var the variable
     * @param pkValue the primary value
     * @param value the value
     * @param isBlob true if field is blob
     * @throws Exception in case of any error
     */
    private void updateStagingBinary(Connection conn, Variable var,
            String pkValue, Object value, boolean isBlob)
        throws Exception
    {
        PreparedStatement statement = null;
        InputStream inStream = null;
        String sql = isBlob ? "insert into "
                + getTempTableName("external_blobs")
                + " (table_name, field_name, pk, clob_field, blob_field) values(?, ?, ?, null, ?)"
                : "insert into "
                        + getTempTableName("external_blobs")
                        + " (table_name, field_name, pk, clob_field, blob_field) values(?, ?, ?, ?, null)";
        
        if (value == null)
            return;
        
        try
        {
            if (isBlob)
                inStream = Utils.getInputStreamFromObject(value);
            
            statement = conn.prepareCall(sql);
            statement.setString(1, var.getTableName());
            statement.setString(2, var.getName());
            statement.setLong(3, Long.parseLong(pkValue));
            
            if (isBlob)
                SqlUtils.setBlob(statement, value, 4);
            else
                SqlUtils.setClob(statement, value, 4);
            
            statement.executeUpdate();
        }
        finally
        {
            SqlUtils.cleanUpSQLData(statement, null, this);
            
            if (inStream != null)
                inStream.close();
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#updateStagingBlob(java.sql.Connection,
     * com.toolsverse.etl.common.Variable, java.lang.String, java.lang.Object)
     */
    public void updateStagingBlob(Connection conn, Variable var,
            String pkValue, Object value)
        throws Exception
    {
        updateStagingBinary(conn, var, pkValue, value, true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#updateStagingClob(java.sql.Connection,
     * com.toolsverse.etl.common.Variable, java.lang.String, java.lang.Object)
     */
    public void updateStagingClob(Connection conn, Variable var,
            String pkValue, Object value)
        throws Exception
    {
        updateStagingBinary(conn, var, pkValue, value, false);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#value2StorageValue(int,
     * java.lang.Object, java.util.Map)
     */
    public Object value2StorageValue(int fieldType, Object value,
            Map<String, String> params)
    {
        return SqlUtils.value2StorageValue(fieldType, value, params);
    }
}
