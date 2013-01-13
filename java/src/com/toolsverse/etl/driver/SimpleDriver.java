/*
 * SimpleDriver.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.driver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.FieldsRepository;
import com.toolsverse.etl.common.OnException;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;

/**
 * The default abstract implementation of the Driver interface which does not
 * support callable statements.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public abstract class SimpleDriver extends AbstractDriver
{
    
    /** The default function class. */
    public static final String DEF_FUNCTION_CLASS = "com.toolsverse.etl.core.function.DefFunctions";
    
    /** The "on exception" sql. */
    public static final String ON_EXCEPTION = "{ON EXCEPTION ";
    
    /** The "end on exception" sql. */
    public static final String END_ON_EXCEPTION = "{END ON EXCEPTION}";
    
    /** The parent driver. */
    private Driver _parent;
    
    /** The parent driver class name. */
    private String _parentDriverName;
    
    /** The override max string literal size. */
    private int _overrideMaxStringLiteralSize;
    
    /** The override max varchar size. */
    private int _overrideMaxVarcharSize;
    
    /** The override max char size. */
    private int _overrideMaxCharSize;
    
    /**
     * Instantiates a new simple driver.
     */
    public SimpleDriver()
    {
        _parent = null;
        
        _parentDriverName = null;
        
        _overrideMaxStringLiteralSize = -1;
        
        _overrideMaxVarcharSize = -1;
        
        _overrideMaxCharSize = -1;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#convertStringForStorage(java.lang.String
     * )
     */
    public String convertStringForStorage(String value)
    {
        if (value == null)
            return null;
        
        int size = getMaxStringLiteralSize();
        
        if (size > 0 && value.length() > size)
            value = value.substring(0, size);
        
        return "'" + value + "'";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#convertValueForStorage(java.lang.Object,
     * int, boolean)
     */
    public String convertValueForStorage(Object fieldValue, int fieldType,
            boolean isFromTable)
    {
        if (_parent != null)
            return _parent.convertValueForStorage(fieldValue, fieldType,
                    isFromTable);
        else
        {
            if (fieldValue == null)
                return "NULL";
            
            String value = SqlUtils.filter(this, fieldValue.toString(),
                    fieldType);
            
            if (SqlUtils.isChar(fieldType) || SqlUtils.isClob(fieldType))
            {
                return convertStringForStorage(value);
            }
            
            switch (fieldType)
            {
                case Types.DATE:
                    if (fieldValue instanceof Date)
                        return "'"
                                + Utils.date2Str((Date)fieldValue,
                                        DataSet.DATA_SET_DATE_ONLY_FORMAT)
                                + "'";
                    else
                        return "'" + value + "'";
                case Types.TIME:
                    if (fieldValue instanceof Date)
                        return "'"
                                + Utils.date2Str((Date)fieldValue,
                                        DataSet.DATA_SET_TIME_FORMAT) + "'";
                    else
                        return "'" + value + "'";
                case Types.TIMESTAMP:
                    if (fieldValue instanceof Date)
                        return "'"
                                + Utils.date2Str((Date)fieldValue,
                                        DataSet.DATA_SET_DATE_TIME_FORMAT)
                                + "'";
                    else
                        return "'" + value + "'";
                default:
                    return value;
            }
        }
    }
    
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
        if (_parent != null)
        {
            _parent.deleteStagingBinary(conn, tableName);
            
            return;
        }
        
        throw new IllegalArgumentException(
                "Function is not supported: deleteStagingBinary");
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#filter(java.lang.String)
     */
    @Override
    public String filter(String value)
    {
        String[] ORIGINAL = {"'"};
        String[] REPLACE_ON = {"''"};
        
        return Utils.filter(value, ORIGINAL, REPLACE_ON);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getBegin()
     */
    public String getBegin()
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getBeginSplited()
     */
    public String getBeginSplited()
    {
        return "";
    }
    
    /**
     * Gets the field definition which matches the best with the given
     * <code>type</code> and <code>typeName</code>.
     *
     * @param fieldsRepository the fields repository
     * @param key the key
     * @param source the source
     * @param type the sql data type
     * @return the field definition
     */
    private FieldDef getBestMatch(FieldsRepository fieldsRepository,
            String key, FieldDef source, Integer type)
    {
        type = type == null ? source.getSqlDataType() : type;
        
        List<FieldDef> fields = fieldsRepository.getFieldDef(key, type);
        
        if (fields == null || fields.size() == 0)
            return null;
        
        FieldDef ret = null;
        
        String typeName = SqlUtils.getJustTypeName(source.getNativeDataType());
        
        for (FieldDef fieldDef : fields)
        {
            if (typeName != null
                    && typeName.equalsIgnoreCase(fieldDef.getNativeDataType()))
            {
                ret = fieldDef;
                
                break;
            }
        }
        
        ret = ret != null ? ret : fields.get(0);
        
        if (SqlUtils.isChar(ret.getSqlDataType()))
        {
            if (source.getPrecision() > 0
                    && source.getPrecision() > ret.getPrecision())
            {
                List<FieldDef> clobs = fieldsRepository.getFieldDef(key,
                        Types.CLOB);
                
                if (clobs != null && clobs.size() > 0)
                {
                    ret = clobs.get(0);
                }
                else
                {
                    
                    List<FieldDef> longvarchars = fieldsRepository.getFieldDef(
                            key, Types.LONGVARCHAR);
                    
                    if (longvarchars != null && longvarchars.size() > 0)
                    {
                        ret = longvarchars.get(0);
                    }
                }
                
            }
        }
        
        return ret;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getBlob(java.sql.ResultSet, int)
     */
    public Object getBlob(ResultSet rs, int pos)
        throws Exception
    {
        if (_parent != null)
            return _parent.getBlob(rs, pos);
        
        return SqlUtils.getBlob(rs, pos);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getCallSql(java.lang.String)
     */
    public String getCallSql(String name)
    {
        if (_parent != null)
            return _parent.getCallSql(name);
        
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getClob(java.sql.ResultSet, int)
     */
    public Object getClob(ResultSet rs, int pos)
        throws Exception
    {
        if (_parent != null)
            return _parent.getClob(rs, pos);
        
        return SqlUtils.getClob(rs, pos);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getCreateTableSql(java.lang.String,
     * com.toolsverse.etl.common.DataSet, boolean, java.lang.String,
     * com.toolsverse.etl.common.FieldsRepository)
     */
    public String getCreateTableSql(String name, DataSet dataSet,
            boolean isTemporary, String key, FieldsRepository fieldsRepository)
    {
        if (_parent != null)
            return _parent.getCreateTableSql(name, dataSet, isTemporary, key,
                    fieldsRepository);
        else
        {
            return "create table "
                    + name
                    + " ("
                    + SqlUtils.getFieldsSql(dataSet, this, key,
                            fieldsRepository) + ");";
            
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getCursorLoopEndSql(java.lang.String,
     * com.toolsverse.etl.common.DataSet)
     */
    public String getCursorLoopEndSql(String name, DataSet dataSet)
    {
        throw new IllegalArgumentException(
                "Function is not supported: getCursorLoopEndSql");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getCursorLoopStartSql(java.lang.String,
     * java.lang.String, com.toolsverse.etl.common.DataSet)
     */
    public String getCursorLoopStartSql(String name, String cursorSql,
            DataSet dataSet)
    {
        throw new IllegalArgumentException(
                "Function is not supported: getCursorLoopStartSql");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getCursorRecAccessSql(java.lang.String)
     */
    public String getCursorRecAccessSql(String fieldName)
    {
        throw new IllegalArgumentException(
                "Function is not supported: getCursorRecAccessSql");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getDeclare()
     */
    public String getDeclare()
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getDeclareCursorEndSql()
     */
    public String getDeclareCursorEndSql()
    {
        throw new IllegalArgumentException(
                "Function is not supported: getDeclareCursorEndSql");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getDeclareCursorSql(java.lang.String,
     * java.lang.String, java.lang.String, com.toolsverse.etl.common.DataSet)
     */
    public String getDeclareCursorSql(String sql, String name,
            String cursorSql, DataSet dataSet)
    {
        throw new IllegalArgumentException(
                "Function is not supported: getDeclareCursorSql");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getDeclareCursorVarSql(java.lang.String,
     * com.toolsverse.etl.common.DataSet, java.lang.String,
     * com.toolsverse.etl.common.FieldsRepository, java.util.Set)
     */
    public String getDeclareCursorVarSql(String sql, DataSet dataSet,
            String key, FieldsRepository fieldsRepository, Set<String> variables)
    {
        throw new IllegalArgumentException(
                "Function is not supported: getDeclareCursorVarSql");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getDefaultFunctionClass()
     */
    public String getDefaultFunctionClass()
    {
        return DEF_FUNCTION_CLASS;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getDefaultType()
     */
    public String getDefaultType()
    {
        if (_parent != null)
            return _parent.getDefaultType();
        
        throw new IllegalArgumentException(
                "Function is not supported: getDefaultType");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getDestinationInfo(java.lang.String)
     */
    public TypedKeyValue<String, Integer> getDestinationInfo(String sql)
    {
        String value = getOnExceptionStr(sql);
        
        if (Utils.isNothing(value))
            return null;
        
        String[] values = value.split("\\|", -1);
        
        String name = values[0];
        int index = values.length > 1 ? Integer.valueOf(values[1]) : -1;
        
        TypedKeyValue<String, Integer> destInfo = new TypedKeyValue<String, Integer>(
                name, index);
        
        return destInfo;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getDropSql(java.lang.String,
     * java.lang.String)
     */
    public String getDropSql(String type, String name)
    {
        if (_parent != null)
            return _parent.getDropSql(type, name);
        
        if (TABLE_TYPE.equalsIgnoreCase(type))
            return "   drop table " + name;
        else
            return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getEnd()
     */
    public String getEnd()
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getEndSplited()
     */
    public String getEndSplited()
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getIf()
     */
    public String getIf()
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getIfBegin()
     */
    public String getIfBegin()
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getIfElse()
     */
    public String getIfElse()
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getIfEnd()
     */
    public String getIfEnd()
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getInitSql()
     */
    public String getInitSql()
    {
        if (_parent != null)
            return _parent.getInitSql();
        else
            return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getLinesLimit()
     */
    public int getLinesLimit()
    {
        return -1;
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
        
        if (_parent != null)
            return _parent.getMaxCharSize();
        else
            throw new IllegalArgumentException(
                    "Function is not supported: getMaxCharSize");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getMaxPrecision()
     */
    public int getMaxPrecision()
    {
        if (_parent != null)
            return _parent.getMaxPrecision();
        
        throw new IllegalArgumentException(
                "Function is not supported: getMaxPrecision");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getMaxScale()
     */
    public int getMaxScale()
    {
        if (_parent != null)
            return _parent.getMaxScale();
        
        throw new IllegalArgumentException(
                "Function is not supported: getMaxScale");
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
        
        if (_parent != null)
            return _parent.getMaxStringLiteralSize();
        else
            return -1;
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
        
        if (_parent != null)
            return _parent.getMaxVarcharSize();
        else
            throw new IllegalArgumentException(
                    "Function is not supported: getMaxVarcharSize");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.AbstractDriver#getMergeStatement(com.toolsverse
     * .util.TypedKeyValue, java.lang.String, java.lang.String)
     */
    @Override
    public String getMergeStatement(
            TypedKeyValue<List<String>, List<String>> fieldsAndValues,
            String tableName, String key)
    {
        if (_parent != null && _parent.isMergeInNonCallableSupported())
        {
            return _parent.getMergeStatement(fieldsAndValues, tableName, key);
        }
        
        throw new IllegalArgumentException(
                "Function is not supported: getMergeStatement");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getMetadataSelectClause()
     */
    public String getMetadataSelectClause()
    {
        if (_parent != null)
            return _parent.getMetadataSelectClause();
        
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getMetadataWhereClause()
     */
    public String getMetadataWhereClause()
    {
        if (_parent != null)
            return _parent.getMetadataWhereClause();
        
        return null;
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
        if (_parent != null)
            return _parent.getObject(rs, index, fieldType);
        
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
     * @see
     * com.toolsverse.etl.driver.Driver#getOnException(com.toolsverse.etl.common
     * .OnException)
     */
    public String getOnException(OnException onException)
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getOnExceptionBegin(com.toolsverse.etl
     * .common.OnException, long)
     */
    public String getOnExceptionBegin(OnException onException, long row)
    {
        return ON_EXCEPTION + onException.getName() + "|" + row + "} ";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getOnExceptionEnd()
     */
    public String getOnExceptionEnd()
    {
        return END_ON_EXCEPTION;
    }
    
    /**
     * Gets the "on exception" string from the sql.
     * 
     * @param sql
     *            the sql
     * @return the "on exception" string
     */
    private String getOnExceptionStr(String sql)
    {
        int start = sql.indexOf(ON_EXCEPTION);
        
        if (start < 0)
            return null;
        
        int end = sql.indexOf("}", start);
        
        if (end < 0)
            return null;
        
        return sql.substring(start + ON_EXCEPTION.length(), end);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getParamType(java.lang.String)
     */
    public int getParamType(String type)
    {
        if (_parent != null)
            return _parent.getParamType(type);
        
        return Types.OTHER;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getParentDriverName()
     */
    public String getParentDriverName()
    {
        return _parentDriverName;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getTempTableName(java.lang.String)
     */
    public String getTempTableName(String name)
    {
        if (_parent != null)
            return _parent.getTempTableName(name);
        
        return name;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getTopSelectClause(int)
     */
    public String getTopSelectClause(int top)
    {
        if (_parent != null)
            return _parent.getTopSelectClause(top);
        
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getTopTrailClause(int)
     */
    public String getTopTrailClause(int top)
    {
        if (_parent != null)
            return _parent.getTopTrailClause(top);
        
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getTopWhereClause(int)
     */
    public String getTopWhereClause(int top)
    {
        if (_parent != null)
            return _parent.getTopWhereClause(top);
        
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getType(com.toolsverse.etl.common.FieldDef
     * , java.lang.String, com.toolsverse.etl.common.FieldsRepository)
     */
    public String getType(FieldDef fieldDef, String key,
            FieldsRepository fieldsRepository)
    {
        if (_parent != null)
            return _parent.getType(fieldDef, key, fieldsRepository);
        
        if (!Utils.isNothing(key) && fieldsRepository != null)
        {
            FieldDef dest = getBestMatch(fieldsRepository, key, fieldDef, null);
            
            if (dest != null)
                return SqlUtils.convertDataType(fieldDef, dest);
            
            int[] fields = SqlUtils.getReplaceableTypes(fieldDef
                    .getSqlDataType());
            
            if (fields != null)
                for (int i = 0; i < fields.length; i++)
                {
                    dest = getBestMatch(fieldsRepository, key, fieldDef,
                            fields[i]);
                    
                    if (dest != null)
                        return SqlUtils.convertDataType(fieldDef, dest);
                }
        }
        
        switch (fieldDef.getSqlDataType())
        {
            case Types.BIT:
                return "NUMBER";
            case Types.TINYINT:
                return "NUMBER";
            case Types.SMALLINT:
                return "NUMBER";
            case Types.INTEGER:
                return "NUMBER";
            case Types.BIGINT:
                return "NUMBER";
            case Types.FLOAT:
                return "NUMBER";
            case Types.REAL:
                return "NUMBER";
            case Types.DOUBLE:
                return "NUMBER";
            case Types.DECIMAL:
            case Types.NUMERIC:
                return "NUMBER";
            case Types.CHAR:
                return "CHAR";
            case Types.LONGVARCHAR:
            case SqlUtils.NCHAR:
            case SqlUtils.NVARCHAR:
            case SqlUtils.LONGNVARCHAR:
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.DATE:
                return "DATE";
            case Types.TIME:
                return "TIME";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.NULL:
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.ARRAY:
            case Types.BLOB:
                return "BLOB";
            case Types.CLOB:
            case SqlUtils.NCLOB:
                return "CLOB";
            case Types.REF:
                return "BLOB";
            case Types.DATALINK:
                return "BLOB";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case SqlUtils.ROWID:
                return "ROWID";
            case SqlUtils.SQLXML:
                return "XMLType";
        }
        
        return fieldDef.getNativeDataType();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getVarDeclare()
     */
    public String getVarDeclare()
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getVarName(java.lang.String)
     */
    public String getVarName(String name)
    {
        if (_parent != null)
            return _parent.getVarName(name);
        
        return name;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getWrongPrecision()
     */
    public int getWrongScale()
    {
        if (_parent != null)
            return _parent.getWrongScale();
        
        return -1;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#ignoreExceptionsDuringInit()
     */
    public boolean ignoreExceptionsDuringInit()
    {
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.AbstractDriver#isMergeInNonCallableSupported()
     */
    @Override
    public boolean isMergeInNonCallableSupported()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#needSeparateConnectionForDdl()
     */
    @Override
    public boolean needSeparateConnectionForDdl()
    {
        if (_parent != null)
            return _parent.needSeparateConnectionForDdl();
        
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#replaceOnException(java.lang.String)
     */
    public String replaceOnException(String sql)
    {
        String value = getOnExceptionStr(sql);
        
        if (value == null)
            return sql;
        
        sql = Utils.findAndReplace(sql, ON_EXCEPTION + value + "}", "", true);
        
        return sql;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#requiresRollbackAfterSqlError()
     */
    @Override
    public boolean requiresRollbackAfterSqlError()
    {
        if (_parent != null)
            return _parent.requiresRollbackAfterSqlError();
        
        return false;
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
        if (_parent != null)
            _parent.setBlob(pstmt, value, pos);
        else
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
        if (_parent != null)
            _parent.setClob(pstmt, value, pos);
        else
            SqlUtils.setClob(pstmt, value, pos);
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setInitSql(java.lang.String)
     */
    public void setInitSql(String value)
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setLinesLimit(int)
     */
    public void setLinesLimit(int value)
    {
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
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setMaxScale(int)
     */
    public void setMaxScale(int value)
    {
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
        if (value != null)
        {
            _parent = (Driver)ObjectFactory.instance().get(value, true);
            
            _parentDriverName = value;
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsAnonymousBlocks()
     */
    public boolean supportsAnonymousBlocks()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsBinaryInProc()
     */
    public boolean supportsBinaryInProc()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsCallableStatement()
     */
    public boolean supportsCallableStatement()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsExplainPlan()
     */
    public boolean supportsExplainPlan()
    {
        return _parent != null ? _parent.supportsExplainPlan() : false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsExternalTool()
     */
    public boolean supportsExternalTool()
    {
        return _parent != null ? _parent.supportsExternalTool() : false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsInnerFunctions()
     */
    public boolean supportsInnerFunctions()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsParamsInAnonymousBlocks()
     */
    public boolean supportsParamsInAnonymousBlocks()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsRollbackAfterDDL()
     */
    public boolean supportsRollbackAfterDDL()
    {
        if (_parent != null)
            return _parent.supportsRollbackAfterDDL();
        
        return true;
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
        if (_parent != null)
        {
            _parent.updateStagingBlob(conn, var, pkValue, value);
            
            return;
        }
        
        throw new IllegalArgumentException(
                "Function is not supported: updateStagingBlob");
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
        if (_parent != null)
        {
            _parent.updateStagingClob(conn, var, pkValue, value);
            
            return;
        }
        
        throw new IllegalArgumentException(
                "Function is not supported: updateStagingClob");
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
        if (_parent != null)
            return _parent.value2StorageValue(fieldType, value, params);
        else
            return SqlUtils.value2StorageValue(fieldType, value, params);
    }
    
}
