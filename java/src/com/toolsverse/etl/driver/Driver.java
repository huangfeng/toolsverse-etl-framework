/*
 * Driver.java
 * 
 * Copyright 2010-2012 Toolsverse . All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.driver;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.FieldsRepository;
import com.toolsverse.etl.common.OnException;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.parser.SqlParser;
import com.toolsverse.ext.ExtensionModule;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.TypedKeyValue;

/**
 * The interface that every ETL driver class must implement. Driver provides translation layer between data and sql created by code generator. 
 * 
 * @see com.toolsverse.ext.ExtensionModule
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface Driver extends ExtensionModule, Serializable
{
    // standard parameters
    /** The server name. */
    static final String SERVER_PARAM = "SERVER";
    
    /** The database name. */
    static final String DB_PARAM = "DB";
    
    /** The command line options. */
    static final String CMD_OPTIONS_PARAM = "CMD_OPTIONS";
    
    /** The BASE_CLASS_PATH. */
    static final String BASE_CLASS_PATH = "com.toolsverse.etl.driver";
    
    // types
    
    /** The procedure. */
    static String PROC_TYPE = "procedure";
    
    /** The function. */
    static String FUNC_TYPE = "function";
    
    /** The ddl. */
    static String DDL_TYPE = "ddl";
    
    /** The table. */
    static String TABLE_TYPE = "table";
    
    // defaults
    
    /** The ETL_CODE. */
    static final String ETL_CODE = "{etl_code}";
    
    /** The case sensitivity undefined. */
    static int CASE_SENSITIVE_UNDEFINED = 0;
    
    /** The lower case. */
    static int CASE_SENSITIVE_LOWER = 1;
    
    /** The upper case. */
    static int CASE_SENSITIVE_UPPER = 2;
    
    /** The lower case property. */
    static String CASE_SENSITIVE_LOWER_STR = "lower";
    
    /** The upper case property. */
    static String CASE_SENSITIVE_UPPER_STR = "upper";
    
    /**
     * Converts string so it can be used as s part of sql. Typically adds "'". Example abc -> 'abc'
     * 
     * @param value
     *            the value
     * 
     * @return the string
     */
    String convertStringForStorage(String value);
    
    /**
     * Converts value to string depending on <code>fieldType</code> so it can be used as s part of sql. 
     *
     * @param fieldValue the field value
     * @param fieldType the field type {@link java.sql.Types}}
     * @param isFromTable if true the value is coming from result set, otherwise - from the database cursor
     * @return the string
     */
    String convertValueForStorage(Object fieldValue, int fieldType,
            boolean isFromTable);
    
    /**
     * Deletes staging table.
     *
     * @param conn the connection
     * @param tableName the table name
     * @throws Exception in case of any error
     */
    void deleteStagingBinary(Connection conn, String tableName)
        throws Exception;
    
    /**
     * Filters string. Usually removes prohibited characters.  
     * 
     * @param value
     *            the value
     * 
     * @return the string
     */
    String filter(String value);
    
    /**
     * Gets the allowed identifier chars.
     *
     * @return the allowed identifier chars
     */
    Set<Character> getAllowedIdentifierChars();
    
    /**
     * Gets the "begin" sql statement.
     * 
     * @return the "begin" sql statemnt
     * 
     */
    String getBegin();
    
    /**
     * Gets the "begin" token for splited sql.
     * 
     * @return the the "begin" token for the splited sql
     */
    String getBeginSplited();
    
    /**
     * Gets the blob from the result set.
     * 
     * @param rs
     *            the result set
     * @param pos
     *            the position ofr the blob field
     * 
     * @return the blob
     * 
     * @throws Exception in case of any error
     */
    Object getBlob(ResultSet rs, int pos)
        throws Exception;
    
    /**
     * Gets the "call" sql. Example: "test" - > call test()
     * 
     * @param name
     *            the name of the procedure
     * 
     * @return the "call" sql
     */
    String getCallSql(String name);
    
    /**
     * Gets the "case sensitive" flag. Possible values: CASE_SENSITIVE_UNDEFINED, CASE_SENSITIVE_LOWER, CASE_SENSITIVE_UPPER.
     * Depending on returning value driver will translate some types of identifiers to the lower case, UPPER case or leave it unchanged. 
     * 
     * @return the "case sensitive" flag. 
     */
    int getCaseSensitive();
    
    /**
     * Gets the CLOB from the result set.
     * 
     * @param rs
     *            the result set
     * @param pos
     *            the position of the clob field
     * 
     * @return the clob
     * 
     * @throws Exception in case of any error
     */
    Object getClob(ResultSet rs, int pos)
        throws Exception;
    
    /**
     * Gets the command line for the external tool.
     *
     * @param storage the object storage
     * @param alias the alias
     * @param sqlFile the sql file
     * @return the command line for the external tool
     * @see com.toolsverse.util.ObjectStorage
     */
    String getCmdForExternalTool(ObjectStorage storage, Alias alias,
            String sqlFile);
    
    /**
     * Gets the "create table" sql.
     *
     * @param name the table name
     * @param dataSet the data set
     * @param isTemporary if true the table is temporary
     * @param key the key field(s)
     * @param fieldsRepository the fields repository
     * @return the "create table" sql
     * @see com.toolsverse.etl.common.FieldsRepository
     */
    String getCreateTableSql(String name, DataSet dataSet, boolean isTemporary,
            String key, FieldsRepository fieldsRepository);
    
    /**
     * Gets the "loop end" sql for the cursor.
     * 
     * @param name
     *            the name of the cusror
     * @param dataSet
     *            the data set
     * 
     * @return the "loop end" sql for the cursor
     */
    String getCursorLoopEndSql(String name, DataSet dataSet);
    
    /**
     * Gets the "loop start" sql for the cursor.
     *
     * @param name the name of the cursor
     * @param cursorSql the cursor sql
     * @param dataSet the data set
     * @return the "loop end" sql for the cursor
     */
    String getCursorLoopStartSql(String name, String cursorSql, DataSet dataSet);
    
    /**
     * Gets the record access sql for the cursor.
     * 
     * @param fieldName
     *            the field name
     * 
     * @return the record access sql for the cursor
     */
    String getCursorRecAccessSql(String fieldName);
    
    /**
     * Gets the "declare" sql.
     * 
     * @return the "declare" sql
     */
    String getDeclare();
    
    /**
     * Gets the "declare cursor end" sql.
     * 
     * @return the "declare cursor end" sql
     */
    String getDeclareCursorEndSql();
    
    /**
     * Gets the "declare cursor" sql.
     *
     * @param sql the sql
     * @param name the name of the cursor
     * @param cursorSql the cursor sql
     * @param dataSet the data set
     * @return the "declare cursor" sql
     */
    String getDeclareCursorSql(String sql, String name, String cursorSql,
            DataSet dataSet);
    
    /**
     * Gets the "declare cursor variable" sql.
     *
     * @param sql the sql
     * @param dataSet the data set
     * @param key the key field(s)
     * @param fieldsRepository the fields repository
     * @param variables the variables
     * @return the "declare cursor variable" sql
     * @see com.toolsverse.etl.common.FieldsRepository
     */
    String getDeclareCursorVarSql(String sql, DataSet dataSet, String key,
            FieldsRepository fieldsRepository, Set<String> variables);
    
    /**
     * Gets the default function class name.
     *
     * @return the default function class name
     * @see com.toolsverse.etl.common.Function
     */
    String getDefaultFunctionClass();
    
    /**
     * Gets the "default null" value. Some databases require default "null", "not null" attributes when creating tables.
     * 
     * @return the "default null" attribute
     */
    String getDefaultNull();
    
    /**
     * Gets the default database type.
     * 
     * @return the default database type
     */
    String getDefaultType();
    
    /**
     * Gets the delete statement.
     *
     * @param fieldsAndValues the fields and values
     * @param tableName the table name
     * @param key the key field(s)
     * @return the delete statement
     */
    String getDeleteStatement(
            TypedKeyValue<List<String>, List<String>> fieldsAndValues,
            String tableName, String key);
    
    /**
     * Gets the default delimiter.
     * 
     * @return the delimiter
     */
    String getDelimiter();
    
    /**
     * Gets the destination name and exception handler type from the sql.
     * 
     * @param sql
     *            the sql
     * 
     * @return the destination name and exception handler type
     */
    TypedKeyValue<String, Integer> getDestinationInfo(String sql);
    
    /**
     * Gets the "drop" sql.
     * 
     * @param type
     *            the object type. Possible values: PROC_TYPE, FUNC_TYPE, DDL_TYPE, TABLE_TYPE
     * @param name
     *            the object name
     * 
     * @return the drop sql
     */
    String getDropSql(String type, String name);
    
    /**
     * Gets the "end" sql.
     * 
     * @return the "end" sql
     */
    String getEnd();
    
    /**
     * Gets the "end" token for the splited sql statement.
     * 
     * @return the the "end" token for the splited sql statement
     */
    String getEndSplited();
    
    /**
     * Gets the "error line" pattern". Used to parse exception and find actual error line.
     *
     * @return the "error line" pattern
     */
    String getErrorLinePattern();
    
    /**
     * Gets the explain plan for the sql.
     *
     * @param storage the object storage
     * @param connection the connection
     * @param alias the alias
     * @param sql the sql
     * @return the explain plan
     * @see com.toolsverse.util.ObjectStorage
     */
    Object getExplainPlan(ObjectStorage storage, Connection connection,
            Alias alias, String sql);
    
    /**
     * Gets the external tool name.
     *
     * @return the external tool name
     */
    String getExternalToolName();
    
    /**
     * Gets the home folder for the native database client.
     *
     * @param storage the object storage
     * @return the home folder
     * @see  com.toolsverse.util.ObjectStorage
     */
    String getHome(ObjectStorage storage);
    
    /**
     * Gets the identifier name.
     *
     * @param name the original object name
     * @param type the type. Possible values: PROC_TYPE, FUNC_TYPE, DDL_TYPE, TABLE_TYPE
     * @return the identifier name
     */
    String getIdentifierName(String name, String type);
    
    /**
     * Gets the "if" token.
     * 
     * @return the "if" token
     */
    String getIf();
    
    /**
     * Gets the "if begin" token.
     * 
     * @return the "if begin" token
     */
    String getIfBegin();
    
    /**
     * Gets the "if else" token.
     * 
     * @return the "if else" token
     */
    String getIfElse();
    
    /**
     * Gets the "if end" token.
     * 
     * @return the "if end" token
     */
    String getIfEnd();
    
    /**
     * Gets the initialization sql. This sql is executed first for the etl scenario.
     * 
     * @return the initialization sql
     */
    String getInitSql();
    
    /**
     * Gets the insert statement.
     *
     * @param fieldsAndValues the fields and values
     * @param tableName the table name
     * @return the insert statement
     */
    String getInsertStatement(
            TypedKeyValue<List<String>, List<String>> fieldsAndValues,
            String tableName);
    
    /**
     * Gets the jdbc driver class name.
     *
     * @return the jdbc driver class name
     */
    String getJdbcDriverClassName();
    
    /**
     * Gets the maximum number of lines in the sql block supported by database.
     * 
     * @return the lines limit
     */
    int getLinesLimit();
    
    /**
     * Gets the maximum character size.
     * 
     * @return the maximum character size
     */
    int getMaxCharSize();
    
    /**
     * Gets the maximum precision.
     * 
     * @return the maximum precision
     */
    int getMaxPrecision();
    
    /**
     * Gets the maximum scale.
     * 
     * @return the maximum scale
     */
    int getMaxScale();
    
    /**
     * Gets the maximum string literal size.
     * 
     * @return the maximum string literal size
     */
    int getMaxStringLiteralSize();
    
    /**
     * Gets the maximum varchar size.
     * 
     * @return the maximum varchar size
     */
    int getMaxVarcharSize();
    
    /**
     * Gets the merge statement.
     *
     * @param fieldsAndValues the fields and values
     * @param tableName the table name
     * @param key the key field(s)
     * @return the merge statement
     */
    String getMergeStatement(
            TypedKeyValue<List<String>, List<String>> fieldsAndValues,
            String tableName, String key);
    
    /**
     * Gets the metadata driver class name.
     *
     * @return the metadata driver class name
     */
    String getMetadataClassName();
    
    /**
     * Gets the metadata "select" clause.
     * 
     * @return the metadata "select" clause
     */
    String getMetadataSelectClause();
    
    /**
     * Gets the metadata "where" clause.
     * 
     * @return the metadata "where" clause
     */
    String getMetadataWhereClause();
    
    /**
     * Gets the name of the driver.
     *
     * @return the name of the driver
     */
    String getName();
    
    /**
     * Gets the object from result set.
     * 
     * @param rs
     *            the result set
     * @param index
     *            the index of the field
     * @param fieldType
     *            the field type {@link java.sql.Types}
     * 
     * @return the object
     * 
     * @throws Exception in case of any error
     */
    Object getObject(ResultSet rs, int index, int fieldType)
        throws Exception;
    
    /**
     * Gets the sql used to check if object exists.
     *
     * @param name the name
     * @return the sql
     */
    String getObjectCheckSql(String name);
    
    /**
     * Gets the "on exception" sql.
     *
     * @param onException the OnException
     * @return the "on exception" sql
     * @see com.toolsverse.etl.common.OnException
     */
    String getOnException(OnException onException);
    
    /**
     * Gets the "on exception begin" sql.
     *
     * @param onException the OnException
     * @param row the row
     * @return the "on exception begin" sql
     * @see com.toolsverse.etl.common.OnException
     */
    String getOnExceptionBegin(OnException onException, long row);
    
    /**
     * Gets the "on exception end" sql.
     * 
     * @return the "on exception end" sql
     */
    String getOnExceptionEnd();
    
    /**
     * Gets the parameter type from the output variable type.
     *
     * @param type the output variable type
     * @return the parameter type
     */
    int getParamType(String type);
    
    /**
     * Gets the parent driver class name.
     * 
     * @return the parent driver class name
     */
    String getParentDriverName();
    
    /**
     * This sql is added after all variables declared.
     *  
     * @return sql
     */
    String getPostDeclareSql();
    
    /**
     * Gets the properties.
     *
     * @return the properties
     */
    String[] getProperties();
    
    /**
     * Gets the safe sql. Some databases (Oracle for example) require that sql is executed inside special construct in order to catch exception.
     * 
     * <pre>
     * For example table abc does not exist
     * 
     * delete from abc --> not safe
     * EXECUTE IMMEDIATE 'delete from abc' --> safe
     * </pre>
     *
     * @param name the name
     * @return the safe sql
     */
    String getSafeSql(String name);
    
    /**
     * Gets the sql for the explain plan.
     *
     * @param sql the original sql
     * @param parser the sql parser
     * @return the sql for the explain plan
     */
    String getSqlForExplainPlan(String sql, SqlParser parser);
    
    /**
     * Gets the sql for external tool.
     *
     * @param alias the alias
     * @param sql the original sql
     * @param parser the sql parser
     * @return the sql for the external tool
     */
    String getSqlForExternalTool(Alias alias, String sql, SqlParser parser);
    
    /**
     * Get start transaction sql.
     *
     * @return the start transaction sql
     */
    String getStartTransactionSql();
    
    /**
     * Gets the table name.
     * 
     * @param name
     *            the name
     * 
     * @return the table name
     */
    String getTableName(String name);
    
    /**
     * Gets the temporary table name.
     * 
     * @param name
     *            the name
     * 
     * @return the temporary table name
     */
    String getTempTableName(String name);
    
    /**
     * Gets the "top select" clause. Used to select first n rows.
     *
     * @param top the maximum number of rows to select
     * @return the "top select" clause
     */
    String getTopSelectClause(int top);
    
    /**
     * Gets the "top trail" clause. Used to select first n rows.
     *
     * @param top the maximum number of rows to select
     * @return the "top trail" clause
     */
    String getTopTrailClause(int top);
    
    /**
     * Gets the "top where" clause. Used to select first n rows.
     *
     * @param top the maximum number of rows to select
     * @return the "top where" clause
     */
    String getTopWhereClause(int top);
    
    /**
     * Gets the native field type.
     *
     * @param fieldDef the field definition
     * @param key the key field(s)
     * @param fieldsRepository the fields repository
     * @return the native field type
     */
    String getType(FieldDef fieldDef, String key,
            FieldsRepository fieldsRepository);
    
    /**
     * Gets the update statement.
     *
     * @param fieldsAndValues the fields and values
     * @param tableName the table name
     * @param key the key field(s)
     * @return the update statement
     */
    String getUpdateStatement(
            TypedKeyValue<List<String>, List<String>> fieldsAndValues,
            String tableName, String key);
    
    /**
     * Gets the jdbc driver url pattern.
     *
     * @return the jdbc driver url pattern
     */
    String getUrlPattern();
    
    /**
     * Gets the variable declare statement.
     *
     * @return the variable declare statement
     */
    String getVarDeclare();
    
    /**
     * Gets the variable name.
     * 
     * @param name
     *            the name
     * 
     * @return the variable name
     */
    String getVarName(String name);
    
    /**
     * Gets the wrong scale. If it is not -1 and jdbc driver returns it the actual scale will be set to -1.
     *
     * @return the wrong precision
     */
    int getWrongScale();
    
    /**
     * The "Ignore exceptions during initialization" flag.
     * 
     * @return true, if any exception thrown during execution of the init sql should be ignored
     */
    boolean ignoreExceptionsDuringInit();
    
    /**
     * Checks if "merge" statement can be not callable. Example: MERGRE INTO...
     *
     * @return true, if "merge" statement can be not callable
     */
    boolean isMergeInNonCallableSupported();
    
    /**
     * Check if driver requires separate connection for ddl statements.
     *
     * @return true, if driver requires separate connection for ddl statements
     */
    boolean needSeparateConnectionForDdl();
    
    /**
     * Replaces sql on exception.
     * 
     * @param sql
     *            the sql
     * 
     * @return the string
     */
    String replaceOnException(String sql);
    
    /**
     * Returns true if database requires rollback after sql error. Example - PostgreSQL.
     * 
     * @return true if database requires rollback after sql error
     */
    boolean requiresRollbackAfterSqlError();
    
    /**
     * Sets the blob field.
     *
     * @param pstmt the prepared statement
     * @param value the value
     * @param pos the position for the blob field
     * @throws Exception in case of any error
     */
    void setBlob(PreparedStatement pstmt, Object value, int pos)
        throws Exception;
    
    /**
     * Sets the case sensitive attribute.
     * 
     * @param value
     *            the new case sensitive attribute. Possible values: CASE_SENSITIVE_UNDEFINED, CASE_SENSITIVE_LOWER, CASE_SENSITIVE_UPPER
     */
    void setCaseSensitive(int value);
    
    /**
     * Sets the clob field.
     *
     * @param pstmt the prepared statement
     * @param value the value
     * @param pos the position of the clob field
     * @throws Exception in case of any error
     */
    void setClob(PreparedStatement pstmt, Object value, int pos)
        throws Exception;
    
    /**
     * Sets the initialization sql.
     * 
     * @param value
     *            the new initialization sql
     */
    void setInitSql(String value);
    
    /**
     * Sets the maximum number of lines in the sql block supported by database.
     * 
     * @param value
     *            the new lines limit
     */
    void setLinesLimit(int value);
    
    /**
     * Sets the maximum char size.
     * 
     * @param value
     *            the new maximum char size
     */
    void setMaxCharSize(int value);
    
    /**
     * Sets the maximum precision.
     * 
     * @param value
     *            the new maximum precision
     */
    void setMaxPrecision(int value);
    
    /**
     * Sets the maximum scale.
     * 
     * @param value
     *            the new maximum scale
     */
    void setMaxScale(int value);
    
    /**
     * Sets the maximum string literal size.
     * 
     * @param value
     *            the new maximum string literal size
     */
    void setMaxStringLiteralSize(int value);
    
    /**
     * Sets the maximum varchar size.
     * 
     * @param value
     *            the new maximum varchar size
     */
    void setMaxVarcharSize(int value);
    
    /**
     * Sets the parent driver class name.
     *
     * @param value the parent driver class name
     * @throws Exception in case of any error
     */
    void setParentDriverName(String value)
        throws Exception;
    
    /**
     * "Supports anonymous blocks" flag.
     * 
     * @return true, if database supports anonymous sql blocks
     */
    boolean supportsAnonymousBlocks();
    
    /**
     * "Supports binary data types in procedures" flag.
     * 
     * @return true, if database supports binary data types in procedures
     */
    boolean supportsBinaryInProc();
    
    /**
     * "Supports callable statement" flag.
     * 
     * @return true, if database supports callable statements
     */
    boolean supportsCallableStatement();
    
    /**
     * "Supports explain plan" flag.
     *
     * @return true, if database supports explain plan
     */
    boolean supportsExplainPlan();
    
    /**
     * "Supports external tool" flag.
     *
     * @return true, if database supports external tool
     */
    boolean supportsExternalTool();
    
    /**
     * "Supports inner functions" flag. Inner function is a function inside anonymous sql block.
     * 
     * @return true, if database supports inner functions
     */
    boolean supportsInnerFunctions();
    
    /**
     * Checks is driver supports not nullable collumns.
     *
     * @return true, if successful
     */
    boolean supportsNotNullable();
    
    /**
     * "Supports parallel extract" flag.
     *
     * @return true, if driver supports parallel extract
     */
    boolean supportsParallelExtract();
    
    /**
     * "Supports parallel load" flag.
     *
     * @return true, if driver supports parallel load
     */
    boolean supportsParallelLoad();
    
    /**
     * "Supports parameters in anonymous blocks" flag.
     *
     * @return true, if database supports parameters in anonymous blocks
     */
    boolean supportsParamsInAnonymousBlocks();
    
    /**
     * Used internally to make a disition is it possible to use a generic jdbc driver with a parent driver.
     *
     * @return true, if successful
     */
    boolean supportsParentDriver();
    
    /**
     * "Supports rollback after ddl" flag.
     * 
     * @return true, if database supports rollback after ddl
     */
    boolean supportsRollbackAfterDDL();
    
    /**
     * If true the database supports extended sql, such as PLSQL, Transact SQL, etc. Used by UI to disable/enable menu items.
     * 
     * @return true, if database database supports extended sql
     */
    
    boolean supportsScripts();
    
    /**
     * Converts table name to name.
     *
     * @param name the table name
     * @return the string
     */
    String tableName2Name(String name);
    
    /**
     * Returns <code>true</code> if <code>type</code> has size. For example: Types.VARCHAR has size, Types.INTEGER -  doesn't.
     *
     * @param type the type {@link java.sql.Types}
     * @return true, if type has size
     */
    boolean typeHasSize(int type);
    
    /**
     * Updates staging blob.
     * 
     * @param conn
     *            the connection
     * @param var
     *            the variable
     * @param pkValue
     *            the primary key value
     * @param value
     *            the value
     * 
     * @throws Exception in case of any error
     */
    void updateStagingBlob(Connection conn, Variable var, String pkValue,
            Object value)
        throws Exception;
    
    /**
     * Updates staging clob.
     * 
     * @param conn
     *            the connection
     * @param var
     *            the variable
     * @param pkValue
     *            the primary key value
     * @param value
     *            the value
     * 
     * @throws Exception in case of any error
     */
    void updateStagingClob(Connection conn, Variable var, String pkValue,
            Object value)
        throws Exception;
    
    /**
     * Converts value for storage.
     *
     * @param fieldType the field type {@link java.sql.Types}
     * @param value the value
     * @param params the parameters
     * @return the object
     */
    Object value2StorageValue(int fieldType, Object value,
            Map<String, String> params);
    
}
