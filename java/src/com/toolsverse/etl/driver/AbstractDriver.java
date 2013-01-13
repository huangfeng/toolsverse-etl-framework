/*
 * AbstractDriver.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.driver;

import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.parser.SqlParser;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.ext.ExtUtils;
import com.toolsverse.ext.ExtensionModule;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * The base abstract implementation of the Driver interface.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public abstract class AbstractDriver implements Driver
{
    
    /** The ALLOWED identifier chars. */
    public static Set<Character> ALLOWED_IDENT_CHARS = new HashSet<Character>();
    
    static
    {
        ALLOWED_IDENT_CHARS.add('.');
        ALLOWED_IDENT_CHARS.add('_');
        ALLOWED_IDENT_CHARS.add('#');
        ALLOWED_IDENT_CHARS.add('@');
        ALLOWED_IDENT_CHARS.add('!');
        ALLOWED_IDENT_CHARS.add('&');
    }
    
    /**
     * Gets the command line options.
     *
     * @param alias the alias
     * @return the command line options
     */
    public static String getCmdOptions(Alias alias)
    {
        if (alias == null)
            return "";
        
        String options = Utils.makeString(Utils.getParam(alias.getParams(),
                CMD_OPTIONS_PARAM));
        
        if (Utils.isNothing(options))
            return "";
        else
            return " " + options + " ";
        
    }
    
    /** The case sensitive flag. */
    private int _caseSensitive = CASE_SENSITIVE_UNDEFINED;
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(ExtensionModule ext)
    {
        return ExtUtils.compareTo(this, ext);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#filter(java.lang.String)
     */
    public String filter(String value)
    {
        String[] ORIGINAL = {"'"};
        String[] REPLACE_ON = {"''"};
        
        return Utils.filter(value, ORIGINAL, REPLACE_ON);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getAllowedIdentifierChars()
     */
    public Set<Character> getAllowedIdentifierChars()
    {
        return ALLOWED_IDENT_CHARS;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getCaseSensitive()
     */
    public int getCaseSensitive()
    {
        return _caseSensitive;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getStartTransactionSql()
     */
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getConfigFileName()
     */
    public String getConfigFileName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getDefaultNull()
     */
    public String getDefaultNull()
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getDeleteStatement(com.toolsverse.util
     * .TypedKeyValue, java.lang.String, java.lang.String)
     */
    public String getDeleteStatement(
            TypedKeyValue<List<String>, List<String>> fieldsAndValues,
            String tableName, String key)
    {
        if (Utils.isNothing(key) || fieldsAndValues.getKey().size() == 0)
            return "";
        
        StringBuilder stringBuilder = new StringBuilder("delete from ").append(
                SqlUtils.name2RightCase(this, tableName)).append("\n");
        
        StringBuilder whereBuilder = new StringBuilder("where ");
        
        String[] keyFields = key.split(",");
        
        int count = fieldsAndValues.getKey().size();
        int whereCount = 0;
        
        for (int i = 0; i < count; i++)
        {
            String field = fieldsAndValues.getKey().get(i);
            
            if (Utils.belongsTo(keyFields, field))
            {
                if (whereCount > 0)
                    whereBuilder.append(" and ");
                
                whereBuilder.append(field).append("=")
                        .append(fieldsAndValues.getValue().get(i));
                
                whereCount++;
            }
        }
        
        if (whereCount == 0)
            return "";
        
        return stringBuilder.append(whereBuilder).append(getDelimiter())
                .append("\n").toString();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getDelimiter()
     */
    public String getDelimiter()
    {
        return ";";
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
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getErrorLinePattern()
     */
    public String getErrorLinePattern()
    {
        return " line ";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getHome(com.toolsverse.util.ObjectStorage
     * )
     */
    public String getHome(ObjectStorage storage)
    {
        return null;
    }
    
    /**
     * Gets the identifier name.
     *
     * @param name the name
     * @param size the size
     * @return the identifier name
     */
    protected String getIdentifierName(String name, int size)
    {
        if (name == null)
            return null;
        
        return Utils.trimLeft(name.trim().replaceAll(" ", ""), size)
                .toLowerCase();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getIdentifierName(java.lang.String,
     * java.lang.String)
     */
    public String getIdentifierName(String name, String type)
    {
        return getIdentifierName(name, 128);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getInsertStatement(com.toolsverse.util
     * .TypedKeyValue, java.lang.String)
     */
    public String getInsertStatement(
            TypedKeyValue<List<String>, List<String>> fieldsAndValues,
            String tableName)
    {
        if (fieldsAndValues == null || fieldsAndValues.getKey().size() == 0)
            return "";
        
        StringBuilder stringBuilder = new StringBuilder("insert into ").append(
                SqlUtils.name2RightCase(this, tableName)).append("(\n");
        
        int count = fieldsAndValues.getKey().size();
        
        // names
        for (int i = 0; i < count; i++)
        {
            stringBuilder.append(SqlUtils.name2RightCase(this, fieldsAndValues
                    .getKey().get(i)));
            
            if (i < count - 1 && count > 1)
                stringBuilder.append(",");
            
            stringBuilder.append("\n");
        }
        
        stringBuilder.append(") values (\n");
        
        // values
        for (int i = 0; i < count; i++)
        {
            stringBuilder.append(fieldsAndValues.getValue().get(i));
            
            if (i < count - 1 && count > 1)
                stringBuilder.append(",");
            
            stringBuilder.append("\n");
        }
        
        return stringBuilder.append(")").append(getDelimiter()).append("\n")
                .toString();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getLocalUnitClassPath()
     */
    public String getLocalUnitClassPath()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getMergeStatement(com.toolsverse.util
     * .TypedKeyValue, java.lang.String, java.lang.String)
     */
    public String getMergeStatement(
            TypedKeyValue<List<String>, List<String>> fieldsAndValues,
            String tableName, String key)
    {
        if (Utils.isNothing(key) || fieldsAndValues.getKey().size() == 0)
            return "";
        
        String insert = getInsertStatement(fieldsAndValues, tableName);
        
        String update = getUpdateStatement(fieldsAndValues, tableName, key);
        
        StringBuilder whereBuilder = new StringBuilder(
                "not exists (select 1 from ").append(
                SqlUtils.name2RightCase(this, tableName)).append(" where ");
        
        String[] keyFields = key.split(",");
        
        int count = fieldsAndValues.getKey().size();
        int whereCount = 0;
        
        for (int i = 0; i < count; i++)
        {
            String field = fieldsAndValues.getKey().get(i);
            
            if (Utils.belongsTo(keyFields, field))
            {
                if (whereCount > 0)
                    whereBuilder.append(" and ");
                
                whereBuilder.append(field).append("=")
                        .append(fieldsAndValues.getValue().get(i));
                
                whereCount++;
            }
        }
        
        if (whereCount == 0)
            return "";
        
        String eof = getDelimiter() + "\n";
        
        return new StringBuilder(getIf()).append("(").append(whereBuilder)
                .append("))").append(getIfBegin()).append(insert)
                .append(getIfElse()).append(update)
                .append(Utils.truncEnd(getIfEnd(), eof)).append(getDelimiter())
                .append("\n").toString();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getObjectCheckSql(java.lang.String)
     */
    public String getObjectCheckSql(String name)
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getPostDeclareSql()
     */
    public String getPostDeclareSql()
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getProperties()
     */
    public String[] getProperties()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getSafeSql(java.lang.String)
     */
    public String getSafeSql(String sql)
    {
        return sql;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getSqlForExplainPlan(java.lang.String,
     * com.toolsverse.etl.parser.SqlParser)
     */
    public String getSqlForExplainPlan(String sql, SqlParser parser)
    {
        return SqlUtils.getSqlForExplainPlan(sql, this, parser);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getStartTransactionSql()
     */
    public String getStartTransactionSql()
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#getTableName(java.lang.String)
     */
    public String getTableName(String name)
    {
        return name;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getType()
     */
    public String getType()
    {
        return "Etl Driver";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.Driver#getUpdateStatement(com.toolsverse.util
     * .TypedKeyValue, java.lang.String, java.lang.String)
     */
    public String getUpdateStatement(
            TypedKeyValue<List<String>, List<String>> fieldsAndValues,
            String tableName, String key)
    {
        if (Utils.isNothing(key) || fieldsAndValues.getKey().size() == 0)
            return "";
        
        StringBuilder stringBuilder = new StringBuilder("update ").append(
                SqlUtils.name2RightCase(this, tableName)).append("\n");
        
        StringBuilder whereBuilder = new StringBuilder("where ");
        
        String[] keyFields = key.split(",");
        
        int count = fieldsAndValues.getKey().size();
        int whereCount = 0;
        int setCount = 0;
        
        for (int i = 0; i < count; i++)
        {
            String field = fieldsAndValues.getKey().get(i);
            
            if (Utils.belongsTo(keyFields, field))
            {
                if (whereCount > 0)
                    whereBuilder.append(" and ");
                
                whereBuilder.append(field).append("=")
                        .append(fieldsAndValues.getValue().get(i));
                
                whereCount++;
            }
            else
            {
                if (setCount > 0)
                    stringBuilder.append(",");
                else
                    stringBuilder.append("set ");
                
                stringBuilder.append(SqlUtils.name2RightCase(this, field))
                        .append("=").append(fieldsAndValues.getValue().get(i))
                        .append("\n");
                
                setCount++;
            }
        }
        
        if (whereCount == 0)
            return "";
        
        return stringBuilder.append(whereBuilder).append(getDelimiter())
                .append("\n").toString();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#isMergeInNonCallableSupported()
     */
    public boolean isMergeInNonCallableSupported()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#needSeparateConnectionForDdl()
     */
    public boolean needSeparateConnectionForDdl()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#requiresRollbackAfterSqlError()
     */
    public boolean requiresRollbackAfterSqlError()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#setCaseSensitive(int)
     */
    public void setCaseSensitive(int value)
    {
        _caseSensitive = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsNotNullable()
     */
    public boolean supportsNotNullable()
    {
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsParallelExtract()
     */
    public boolean supportsParallelExtract()
    {
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsParallelLoad()
     */
    public boolean supportsParallelLoad()
    {
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#supportsParentDriver()
     */
    public boolean supportsParentDriver()
    {
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#tableName2Name(java.lang.String)
     */
    public String tableName2Name(String name)
    {
        return name;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.driver.Driver#typeHasSize(int)
     */
    public boolean typeHasSize(int type)
    {
        switch (type)
        {
            case Types.BINARY:
                return false;
            case Types.BIT:
                return false;
            case Types.TINYINT:
                return false;
            case Types.SMALLINT:
                return false;
            case Types.INTEGER:
                return false;
            case Types.BIGINT:
                return false;
            case Types.FLOAT:
                return false;
            case Types.REAL:
                return false;
            case Types.DOUBLE:
                return false;
            case Types.DECIMAL:
            case Types.NUMERIC:
                return true;
            case Types.CHAR:
            case Types.LONGVARCHAR:
            case SqlUtils.NCHAR:
            case SqlUtils.NVARCHAR:
            case SqlUtils.LONGNVARCHAR:
            case Types.VARCHAR:
                return true;
            case Types.DATE:
                return false;
            case Types.TIME:
                return false;
            case Types.TIMESTAMP:
                return false;
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.JAVA_OBJECT:
            case Types.BLOB:
            case Types.CLOB:
            case SqlUtils.NCLOB:
            case Types.STRUCT:
            case Types.ARRAY:
            case Types.REF:
            case Types.BOOLEAN:
                return false;
            case SqlUtils.ROWID:
                return true;
            case SqlUtils.SQLXML:
                return false;
            case Types.OTHER:
            case Types.NULL:
            case Types.DISTINCT:
            case Types.DATALINK:
                return false;
            default:
                return false;
        }
    }
    
}
