/*
 * MockCallableDriver.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.driver;

import java.sql.Connection;
import java.sql.Types;
import java.util.Set;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.FieldsRepository;
import com.toolsverse.etl.common.OnException;
import com.toolsverse.etl.parser.SqlParser;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.Utils;

/**
 * <code>MockCallableDriver</code> is the class that
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @see
 * @since 1.0
 */

public class MockCallableDriver extends CallableDriver
{
    private static final String FUNCTION_CLASS = "com.toolsverse.etl.core.function.MockFunctions";
    private static final String JDBC_DRIVER_CLASS = "";
    private static final String JDBC_URL = "";
    private static final String METADATA_CLASS = "com.toolsverse.etl.metadata.JdbcMetadata";
    
    public static int MAX_STRING_LITERAL_SIZE = 8000;
    public static int MAX_VARCHAR_SIZE = 8000;
    public static int LINES_LIMIT = 7000;
    public static int MAX_CHAR_SIZE = 8000;
    public static int MAX_NUMBER_PREC_SIZE = 38;
    public static int MAX_NUMBER_SCALE_SIZE = 127;
    private static String DATE_TIME_STYLE = "120";
    
    public static final String INIT_SQL = "CREATE TABLE ##primary_keys_table (field_name varchar(100), pk NUMERIC(32), old_pk NUMERIC(32)); \n"
            + "CREATE INDEX pkt_1 on ##primary_keys_table (field_name, old_pk);\n"
            + "CREATE INDEX pkt_2 on ##primary_keys_table (field_name, pk);\n"
            + "CREATE TABLE ##external_blobs (table_name varchar(100), field_name varchar(100), pk numeric(32), clob_field TEXT, blob_field IMAGE);\n"
            + "CREATE INDEX eb_1 on ##external_blobs (table_name, field_name, pk);";
    
    public static final String DECLARE = "  declare @v_dummy int; \n";
    
    public static final String BEGIN = "BEGIN \n" + "set @v_dummy = 0; \n";
    
    public static final String BEGIN_SPLITED = BEGIN;
    
    public static final String END = "end;  \n";
    
    public static final String END_SPLITED = END;
    
    public static final String IF = "if ";
    public static final String IF_BEGIN = " then\n";
    public static final String IF_ELSE = " else\n";
    public static final String IF_END = " end if;\n";
    
    public static final String EXCEPTIONS = " SQLWARNING, NOT FOUND, SQLEXCEPTION ";
    public static final String ON_EXCEPTION_BEGIN = " begin try \n";
    public static final String ON_EXCEPTION = " end try \n" + "begin catch ";
    public static final String ON_EXCEPTION_END = " end catch \n";
    
    @Override
    protected String _getInitSql()
    {
        return INIT_SQL;
    }
    
    @Override
    protected int _getLinesLimit()
    {
        return LINES_LIMIT;
    }
    
    @Override
    protected int _getMaxCharSize()
    {
        return MAX_CHAR_SIZE;
    }
    
    @Override
    protected int _getMaxPrecision()
    {
        return MAX_NUMBER_PREC_SIZE;
    }
    
    @Override
    protected int _getMaxScale()
    {
        return MAX_NUMBER_SCALE_SIZE;
    }
    
    @Override
    protected int _getMaxStringLiteralSize()
    {
        return MAX_STRING_LITERAL_SIZE;
    }
    
    @Override
    protected int _getMaxVarcharSize()
    {
        return MAX_VARCHAR_SIZE;
    }
    
    public String convertStringForStorage(String value)
    {
        if (value == null)
            return null;
        
        return "'" + value + "'";
    }
    
    public String convertValueForStorage(Object fieldValue, int fieldType,
            boolean isFromTable)
    {
        if (fieldValue == null)
            return "NULL";
        
        if (isFromTable)
            return fieldValue.toString();
        
        String value = SqlUtils.filter(this, fieldValue.toString(), fieldType);
        
        switch (fieldType)
        {
            case Types.VARCHAR:
            case Types.CHAR:
            case SqlUtils.NCHAR:
            case SqlUtils.NVARCHAR:
            case SqlUtils.SQLXML:
            case SqlUtils.NCLOB:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                return convertStringForStorage(value);
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return "convert(datetime, '"
                        + SqlUtils.dateTime2Str(fieldValue, null) + "', "
                        + getDateTimeStyle() + ")";
            default:
                return value;
        }
    }
    
    public String filter(String value)
    {
        String[] ORIGINAL = {"'"};
        String[] REPLACE_ON = {"''"};
        
        return Utils.filter(value, ORIGINAL, REPLACE_ON);
    }
    
    public String getBegin()
    {
        return BEGIN;
    }
    
    public String getBeginSplited()
    {
        return BEGIN_SPLITED;
    }
    
    public String getCallSql(String name)
    {
        return "";
    }
    
    public String getCmdForExternalTool(ObjectStorage storage, Alias alias,
            String sqlFile)
    {
        return null;
    }
    
    public String getCreateTableSql(String name, DataSet dataSet,
            boolean isTemporary, String key, FieldsRepository fieldsRepository)
    {
        String prefix = "";
        
        if (isTemporary && name.indexOf("##") != 0)
            prefix = "##";
        
        String sql = "create " + " table " + prefix + name + " ("
                + SqlUtils.getFieldsSql(dataSet, this, key, fieldsRepository)
                + ");";
        
        return sql;
    }
    
    public String getCursorLoopEndSql(String name, DataSet dataSet)
    {
        String sql = getFetchNextSql(name, dataSet) + "END;\n" + "CLOSE "
                + name + "_cur; \n" + "DEALLOCATE " + name + "_cur"
                + getDelimiter() + "\n";
        
        return sql;
    }
    
    public String getCursorLoopStartSql(String name, String cursorSql,
            DataSet dataSet)
    {
        String sql = "OPEN " + name + "_cur" + getDelimiter() + "\n"
                + getFetchNextSql(name, dataSet)
                + "WHILE @@FETCH_STATUS = 0 \n" + "BEGIN \n";
        
        return sql;
    }
    
    public String getCursorRecAccessSql(String fieldName)
    {
        return getCursorVarName(fieldName);
    }
    
    public String getCursorVarName(String field)
    {
        return "@c" + field;
    }
    
    protected String getDateTimeStyle()
    {
        return DATE_TIME_STYLE;
    }
    
    public String getDeclare()
    {
        return DECLARE;
    }
    
    public String getDeclareCursorEndSql()
    {
        return "";
    }
    
    public String getDeclareCursorSql(String sql, String name,
            String cursorSql, DataSet dataSet)
    {
        String declare = "DECLARE " + name
                + "_cur CURSOR LOCAL FAST_FORWARD FOR select * from " + name
                + "; \n";
        
        if (sql.indexOf(declare) < 0)
            sql = sql + declare;
        
        return sql;
    }
    
    public String getDeclareCursorVarSql(String sql, DataSet dataSet,
            String key, FieldsRepository fieldsRepository, Set<String> variables)
    {
        int count = dataSet.getFieldCount();
        
        for (int i = 0; i < count; i++)
        {
            FieldDef fieldDef = dataSet.getFieldDef(i);
            
            String field = fieldDef.getName();
            
            field = "DECLARE " + getCursorVarName(field) + "  "
                    + getType(fieldDef, key, fieldsRepository) + getDelimiter()
                    + "\n";
            
            if (sql.indexOf(field) < 0)
                sql = sql + field;
        }
        
        return sql;
    }
    
    public String getDefaultFunctionClass()
    {
        return FUNCTION_CLASS;
    }
    
    public String getDefaultType()
    {
        return "NUMERIC";
    }
    
    public String getDropSql(String type, String name)
    {
        if (PROC_TYPE.equalsIgnoreCase(type))
            return "   drop procedure " + name;
        if (FUNC_TYPE.equalsIgnoreCase(type))
            return "   drop function " + name;
        else if (TABLE_TYPE.equalsIgnoreCase(type))
            return "   drop table " + name;
        else
            return "";
    }
    
    public String getEnd()
    {
        return END;
    }
    
    public String getEndSplited()
    {
        return END_SPLITED;
    }
    
    public String getExplainPlan(ObjectStorage storage, Connection connection,
            Alias alias, String sql)
    {
        return null;
    }
    
    public String getExternalToolName()
    {
        return null;
    }
    
    public String getFetchNextSql(String name, DataSet dataSet)
    {
        int count = dataSet.getFieldCount();
        
        String sql = "FETCH NEXT FROM " + name + "_cur INTO ";
        
        for (int i = 0; i < count; i++)
        {
            FieldDef fieldDef = dataSet.getFieldDef(i);
            
            String field = getCursorVarName(fieldDef.getName());
            if (i < count - 1)
                field = field + ",";
            
            sql = sql + field;
        }
        
        return sql + getDelimiter() + "\n";
        
    }
    
    public String getIconPath()
    {
        return null;
    }
    
    public String getIf()
    {
        return IF;
    }
    
    public String getIfBegin()
    {
        return IF_BEGIN;
    }
    
    public String getIfElse()
    {
        return IF_ELSE;
    }
    
    public String getIfEnd()
    {
        return IF_END;
    }
    
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
    
    public String getMetadataClassName()
    {
        return METADATA_CLASS;
    }
    
    public String getMetadataSelectClause()
    {
        return getTopSelectClause(0);
    }
    
    public String getMetadataWhereClause()
    {
        return null;
    }
    
    public String getName()
    {
        return "Mock Server";
    }
    
    public String getOnException(OnException onException)
    {
        String exception = onException.getExceptionMask();
        
        if (Utils.isNothing(exception))
            exception = "\n";
        else
            exception = exception + " \n";
        
        return ON_EXCEPTION + exception + "  set @v_dummy = 1; \n";
    }
    
    public String getOnExceptionBegin(OnException onException, long row)
    {
        return ON_EXCEPTION_BEGIN + "set @v_dummy = 0; \n";
    }
    
    public String getOnExceptionEnd()
    {
        return ON_EXCEPTION_END;
    }
    
    public int getParamType(String type)
    {
        return java.sql.Types.OTHER;
    }
    
    public String getSqlForExternalTool(Alias alias, String sql,
            SqlParser parser)
    {
        return null;
    }
    
    @Override
    public String getTableName(String name)
    {
        return name;
    }
    
    public String getTempTableName(String name)
    {
        if (!Utils.isNothing(name) && name.indexOf("##") == 0)
            return name;
        
        return "##" + name;
    }
    
    public String getTopSelectClause(int top)
    {
        return "select top " + top + " ";
    }
    
    public String getTopTrailClause(int top)
    {
        return null;
    }
    
    public String getTopWhereClause(int top)
    {
        return null;
    }
    
    public String getType(FieldDef fieldDef, String key,
            FieldsRepository fieldsRepository)
    {
        switch (fieldDef.getSqlDataType())
        {
            case Types.BIT:
                return "SMALLINT";
            case Types.TINYINT:
                return "TINYINT";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.INTEGER:
                return "INT";
            case Types.BIGINT:
                return "BIGINT";
            case Types.FLOAT:
                return "FLOAT";
            case Types.REAL:
                return "REAL";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.NUMERIC:
            case Types.DECIMAL:
                return "NUMERIC"
                        + SqlUtils.getTypeRange(fieldDef.getNativeDataType(),
                                getMaxPrecision(), getMaxScale());
            case Types.CHAR:
                return "CHAR"
                        + SqlUtils.getTypeRange(fieldDef.getNativeDataType(),
                                getMaxCharSize(), -1);
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case SqlUtils.ROWID:
            case SqlUtils.NCHAR:
            case SqlUtils.NVARCHAR:
                return "VARCHAR"
                        + SqlUtils.getTypeRange(fieldDef.getNativeDataType(),
                                getMaxVarcharSize(), -1);
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return "DATETIME";
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
            case Types.REF:
            case Types.DATALINK:
                return "VARBINARY(max)";
            case Types.CLOB:
            case SqlUtils.NCLOB:
            case SqlUtils.SQLXML:
                return "VARCHAR(max)";
            case Types.BOOLEAN:
                return "INT";
        }
        
        return fieldDef.getNativeDataType();
    }
    
    public String getUrlPattern()
    {
        return JDBC_URL;
    }
    
    public String getVarDeclare()
    {
        return "  declare ";
    }
    
    @Override
    public String getVarName(String name)
    {
        if (name == null)
            return null;
        
        return "@" + name;
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
    
    public String getVersion()
    {
        return "2.0.1";
    }
    
    public String getXmlConfigFileName()
    {
        return null;
    }
    
    public boolean ignoreExceptionsDuringInit()
    {
        return true;
    }
    
    public boolean requiresDiscovery()
    {
        return false;
    }
    
    public boolean supportsAnonymousBlocks()
    {
        return true;
    }
    
    public boolean supportsBinaryInProc()
    {
        return true;
    }
    
    public boolean supportsCallableStatement()
    {
        return true;
    }
    
    public boolean supportsExplainPlan()
    {
        return true;
    }
    
    public boolean supportsExternalTool()
    {
        return true;
    }
    
    public boolean supportsInnerFunctions()
    {
        return false;
    }
    
    public boolean supportsParamsInAnonymousBlocks()
    {
        return true;
    }
    
    public boolean supportsRollbackAfterDDL()
    {
        return true;
    }
    
}
