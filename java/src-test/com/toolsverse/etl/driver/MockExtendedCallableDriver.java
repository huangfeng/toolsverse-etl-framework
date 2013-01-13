/*
 * MockExtendedCallableDriver.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.driver;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.FieldsRepository;
import com.toolsverse.etl.common.OnException;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.Utils;

/**
 * <code>MockExtendedCallableDriver</code> is the class that
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @see
 * @since 1.0
 */

public class MockExtendedCallableDriver extends MockCallableDriver
{
    private static final String FUNCTION_CLASS = "com.toolsverse.etl.core.function.MockExtendedFunctions";
    private static final String JDBC_DRIVER_CLASS = "";
    private static final String JDBC_URL = "";
    private static final String METADATA_CLASS = "com.toolsverse.etl.metadata.JdbcMetadata";
    
    public static int MAX_STRING_LITERAL_SIZE = 16384;
    public static int MAX_VARCHAR_SIZE = 16384;
    public static int LINES_LIMIT = 5000;
    public static int MAX_CHAR_SIZE = 8000;
    public static int MAX_NUMBER_PREC_SIZE = 38;
    public static int MAX_NUMBER_SCALE_SIZE = 127;
    private static String DATE_TIME_STYLE = "110";
    
    public static final String INIT_SQL = "CREATE TABLE #primary_keys_table (field_name varchar(100), pk NUMERIC(32) null, old_pk NUMERIC(32) null) \n"
            + "CREATE INDEX pkt_1 on #primary_keys_table (field_name, old_pk)\n"
            + "CREATE INDEX pkt_2 on #primary_keys_table (field_name, pk)\n"
            + "CREATE TABLE #external_blobs (table_name varchar(100), field_name varchar(100), pk numeric(32), clob_field TEXT null, blob_field IMAGE null)\n"
            + "CREATE INDEX eb_1 on #external_blobs (table_name, field_name, pk)";
    
    public static final String DECLARE = "CREATE PROCEDURE " + ETL_CODE + " \n"
            + "as \n" + "  declare @v_dummy int \n";
    
    public static final String BEGIN = "BEGIN \n" + "select @v_dummy = 0 \n";
    
    public static final String BEGIN_SPLITED = BEGIN;
    
    public static final String END = "end  \n";
    
    public static final String END_SPLITED = END;
    
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
    
    @Override
    public String getBegin()
    {
        return BEGIN;
    }
    
    @Override
    public String getBeginSplited()
    {
        return BEGIN_SPLITED;
    }
    
    @Override
    public String getCallSql(String name)
    {
        return "   " + name;
    }
    
    @Override
    public String getCreateTableSql(String name, DataSet dataSet,
            boolean isTemporary, String key, FieldsRepository fieldsRepository)
    {
        String prefix = "";
        
        if (isTemporary && name.indexOf("#") != 0)
            prefix = "#";
        
        String sql = "create " + " table " + prefix + name + " ("
                + SqlUtils.getFieldsSql(dataSet, this, key, fieldsRepository)
                + ")";
        
        return sql;
    }
    
    @Override
    public String getCursorLoopEndSql(String name, DataSet dataSet)
    {
        String sql = getFetchNextSql(name, dataSet) + "END\n" + "CLOSE " + name
                + "_cur \n" + "DEALLOCATE CURSOR " + name + "_cur \n";
        
        return sql;
    }
    
    @Override
    public String getCursorLoopStartSql(String name, String cursorSql,
            DataSet dataSet)
    {
        String sql = "OPEN " + name + "_cur \n"
                + getFetchNextSql(name, dataSet) + "WHILE (@@sqlstatus=0) \n"
                + "BEGIN \n";
        
        return sql;
    }
    
    @Override
    protected String getDateTimeStyle()
    {
        return DATE_TIME_STYLE;
    }
    
    @Override
    public String getDeclare()
    {
        return DECLARE;
    }
    
    @Override
    public String getDeclareCursorSql(String sql, String name,
            String cursorSql, DataSet dataSet)
    {
        String declare = "declare " + name
                + "_cur no scroll cursor for select * from " + name
                + " for read only \n";
        
        if (sql.indexOf(declare) < 0)
            sql = sql + declare;
        
        return sql;
    }
    
    @Override
    public String getDefaultFunctionClass()
    {
        return FUNCTION_CLASS;
    }
    
    @Override
    public String getDefaultNull()
    {
        return "null";
    }
    
    @Override
    public String getDelimiter()
    {
        return " ";
    }
    
    @Override
    public String getEnd()
    {
        return END;
    }
    
    @Override
    public String getEndSplited()
    {
        return END_SPLITED;
    }
    
    @Override
    public String getJdbcDriverClassName()
    {
        return JDBC_DRIVER_CLASS;
    }
    
    @Override
    public String getMetadataClassName()
    {
        return METADATA_CLASS;
    }
    
    @Override
    public String getName()
    {
        return "Mock Extended Server";
    }
    
    @Override
    public String getOnException(OnException onException)
    {
        String exception = onException.getExceptionMask();
        
        if (Utils.isNothing(exception))
            exception = "\n";
        else
            exception = exception + " \n";
        
        return ON_EXCEPTION + exception + "  select @v_dummy = 1 \n";
    }
    
    public String getOnExceptionBegin(OnException onException)
    {
        return ON_EXCEPTION_BEGIN + "select @v_dummy = 0 \n";
    }
    
    @Override
    public String getTempTableName(String name)
    {
        if (!Utils.isNothing(name) && name.indexOf("#") == 0)
            return name;
        
        return "#" + name;
    }
    
    @Override
    public String getType(FieldDef fieldDef, String key,
            FieldsRepository fieldsRepository)
    {
        if (SqlUtils.isLargeObject(fieldDef.getSqlDataType()))
            return "";
        else
            return super.getType(fieldDef, key, fieldsRepository);
    }
    
    @Override
    public String getUrlPattern()
    {
        return JDBC_URL;
    }
    
    @Override
    public boolean supportsAnonymousBlocks()
    {
        return false;
    }
    
    @Override
    public boolean supportsBinaryInProc()
    {
        return false;
    }
    
}
