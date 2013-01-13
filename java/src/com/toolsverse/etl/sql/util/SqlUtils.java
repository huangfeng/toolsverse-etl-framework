/*
 * SqlUtils.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.FieldsRepository;
import com.toolsverse.etl.common.OnException;
import com.toolsverse.etl.connector.sql.SqlConnector;
import com.toolsverse.etl.connector.sql.SqlConnectorParams;
import com.toolsverse.etl.connector.text.TextConnector;
import com.toolsverse.etl.connector.text.TextConnectorParams;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.parser.GenericSqlParser;
import com.toolsverse.etl.parser.SqlParser;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.DateUtil;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * Collection of static methods for SQL operations.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public final class SqlUtils
{
    // types
    /** The ROWID type. */
    public final static int ROWID = -8;
    
    /** The NCHAR type. */
    public static final int NCHAR = -15;
    
    /** The NVARCHAR type. */
    public static final int NVARCHAR = -9;
    
    /** The LONGNVARCHAR type. */
    public static final int LONGNVARCHAR = -16;
    
    /** The NCLOB type. */
    public static final int NCLOB = 2011;
    
    /** The SQLXML type. */
    public static final int SQLXML = 2009;
    
    /** The CLOB_OUTPUT_PARAM_TYPES. */
    private static final Class<?>[] CLOB_OUTPUT_PARAM_TYPES = new Class[0];
    
    /** The CLOB_OUTPUT_PARAMS. */
    private static final Object[] CLOB_OUTPUT_PARAMS = new Object[0];
    
    /** The BLOB_OUTPUT_METHOD. */
    private static final String BLOB_OUTPUT_METHOD = "getBinaryOutputStream";
    
    /** The BLOB_OUTPUT_PARAM_TYPES. */
    private static final Class<?>[] BLOB_OUTPUT_PARAM_TYPES = CLOB_OUTPUT_PARAM_TYPES;
    
    /** The BLOB_OUTPUT_PARAMS. */
    private static final Object[] BLOB_OUTPUT_PARAMS = CLOB_OUTPUT_PARAMS;
    
    /** The DEFAULT_DELIMITER. */
    public static final String DEFAULT_DELIMITER = "|";
    
    /** The DATE_FORMAT property. */
    public static final String DATE_FORMAT_PROP = "date";
    
    /** The TIME_FORMAT property. */
    public static final String TIME_FORMAT_PROP = "time";
    
    /** The DATE_TIME_FORMAT property. */
    public static final String DATE_TIME_FORMAT_PROP = "datetime";
    
    /** The CHAR SEPARATOR property. */
    public static final String CHAR_SEPARATOR_PROP = "charseparator";
    
    /** The DECIMAL_AS_INT property. */
    public static final String DECIMAL_AS_INT_PROP = "decimalasint";
    
    /** The compatible types_ types. */
    private static Map<String, Integer> COMPAT_TYPES = new HashMap<String, Integer>();
    
    /** The replaceable types. */
    private static Map<Integer, int[]> REPLACEABLE_TYPES = new HashMap<Integer, int[]>();
    
    static
    {
        COMPAT_TYPES.put(Types.CHAR + "|" + Types.LONGVARCHAR, Types.CHAR);
        COMPAT_TYPES.put(Types.CHAR + "|" + SqlUtils.NCHAR, Types.CHAR);
        COMPAT_TYPES.put(Types.CHAR + "|" + SqlUtils.NVARCHAR, Types.CHAR);
        COMPAT_TYPES.put(Types.CHAR + "|" + SqlUtils.LONGNVARCHAR, Types.CHAR);
        COMPAT_TYPES.put(Types.CHAR + "|" + Types.VARCHAR, Types.CHAR);
        
        COMPAT_TYPES
                .put(Types.VARCHAR + "|" + Types.LONGVARCHAR, Types.VARCHAR);
        COMPAT_TYPES.put(Types.VARCHAR + "|" + SqlUtils.NCHAR, Types.VARCHAR);
        COMPAT_TYPES
                .put(Types.VARCHAR + "|" + SqlUtils.NVARCHAR, Types.VARCHAR);
        COMPAT_TYPES.put(Types.VARCHAR + "|" + SqlUtils.LONGNVARCHAR,
                Types.VARCHAR);
        COMPAT_TYPES.put(Types.VARCHAR + "|" + Types.CHAR, Types.VARCHAR);
        
        COMPAT_TYPES.put(Types.LONGVARCHAR + "|" + SqlUtils.LONGNVARCHAR,
                Types.LONGVARCHAR);
        COMPAT_TYPES.put(Types.LONGVARCHAR + "|" + SqlUtils.NCHAR,
                Types.LONGVARCHAR);
        COMPAT_TYPES.put(Types.LONGVARCHAR + "|" + SqlUtils.NVARCHAR,
                Types.LONGVARCHAR);
        COMPAT_TYPES.put(Types.LONGVARCHAR + "|" + Types.VARCHAR,
                Types.LONGVARCHAR);
        COMPAT_TYPES.put(Types.LONGVARCHAR + "|" + Types.CHAR,
                Types.LONGVARCHAR);
        
        COMPAT_TYPES.put(SqlUtils.NCHAR + "|" + Types.LONGVARCHAR,
                SqlUtils.NCHAR);
        COMPAT_TYPES.put(SqlUtils.NCHAR + "|" + SqlUtils.LONGNVARCHAR,
                SqlUtils.NCHAR);
        COMPAT_TYPES.put(SqlUtils.NCHAR + "|" + SqlUtils.NVARCHAR,
                SqlUtils.NCHAR);
        COMPAT_TYPES.put(SqlUtils.NCHAR + "|" + Types.VARCHAR, SqlUtils.NCHAR);
        COMPAT_TYPES.put(SqlUtils.NCHAR + "|" + Types.CHAR, SqlUtils.NCHAR);
        
        COMPAT_TYPES.put(SqlUtils.NVARCHAR + "|" + Types.LONGVARCHAR,
                SqlUtils.NVARCHAR);
        COMPAT_TYPES.put(SqlUtils.NVARCHAR + "|" + SqlUtils.LONGNVARCHAR,
                SqlUtils.NVARCHAR);
        COMPAT_TYPES.put(SqlUtils.NVARCHAR + "|" + SqlUtils.NCHAR,
                SqlUtils.NVARCHAR);
        COMPAT_TYPES.put(SqlUtils.NVARCHAR + "|" + Types.VARCHAR,
                SqlUtils.NVARCHAR);
        COMPAT_TYPES.put(SqlUtils.NVARCHAR + "|" + Types.CHAR,
                SqlUtils.NVARCHAR);
        
        COMPAT_TYPES.put(SqlUtils.LONGNVARCHAR + "|" + Types.LONGVARCHAR,
                SqlUtils.LONGNVARCHAR);
        COMPAT_TYPES.put(SqlUtils.LONGNVARCHAR + "|" + SqlUtils.NVARCHAR,
                SqlUtils.LONGNVARCHAR);
        COMPAT_TYPES.put(SqlUtils.LONGNVARCHAR + "|" + SqlUtils.NCHAR,
                SqlUtils.LONGNVARCHAR);
        COMPAT_TYPES.put(SqlUtils.LONGNVARCHAR + "|" + Types.VARCHAR,
                SqlUtils.LONGNVARCHAR);
        COMPAT_TYPES.put(SqlUtils.LONGNVARCHAR + "|" + Types.CHAR,
                SqlUtils.LONGNVARCHAR);
        
        COMPAT_TYPES.put(Types.DATE + "|" + Types.TIMESTAMP, Types.DATE);
        COMPAT_TYPES.put(Types.DATE + "|" + Types.TIME, Types.DATE);
        
        COMPAT_TYPES.put(Types.TIME + "|" + Types.TIMESTAMP, Types.TIME);
        COMPAT_TYPES.put(Types.TIME + "|" + Types.DATE, Types.TIME);
        
        COMPAT_TYPES.put(Types.TIMESTAMP + "|" + Types.DATE, Types.TIMESTAMP);
        COMPAT_TYPES.put(Types.TIMESTAMP + "|" + Types.TIME, Types.TIMESTAMP);
        
        COMPAT_TYPES.put(Types.NUMERIC + "|" + Types.BIT, Types.NUMERIC);
        COMPAT_TYPES.put(Types.NUMERIC + "|" + Types.TINYINT, Types.NUMERIC);
        COMPAT_TYPES.put(Types.NUMERIC + "|" + Types.SMALLINT, Types.NUMERIC);
        COMPAT_TYPES.put(Types.NUMERIC + "|" + Types.INTEGER, Types.NUMERIC);
        COMPAT_TYPES.put(Types.NUMERIC + "|" + Types.BIGINT, Types.NUMERIC);
        COMPAT_TYPES.put(Types.NUMERIC + "|" + Types.FLOAT, Types.NUMERIC);
        COMPAT_TYPES.put(Types.NUMERIC + "|" + Types.REAL, Types.NUMERIC);
        COMPAT_TYPES.put(Types.NUMERIC + "|" + Types.DECIMAL, Types.NUMERIC);
        COMPAT_TYPES.put(Types.NUMERIC + "|" + Types.DOUBLE, Types.NUMERIC);
        
        COMPAT_TYPES.put(Types.DOUBLE + "|" + Types.BIT, Types.DOUBLE);
        COMPAT_TYPES.put(Types.DOUBLE + "|" + Types.TINYINT, Types.DOUBLE);
        COMPAT_TYPES.put(Types.DOUBLE + "|" + Types.SMALLINT, Types.DOUBLE);
        COMPAT_TYPES.put(Types.DOUBLE + "|" + Types.INTEGER, Types.DOUBLE);
        COMPAT_TYPES.put(Types.DOUBLE + "|" + Types.BIGINT, Types.DOUBLE);
        COMPAT_TYPES.put(Types.DOUBLE + "|" + Types.FLOAT, Types.DOUBLE);
        COMPAT_TYPES.put(Types.DOUBLE + "|" + Types.REAL, Types.DOUBLE);
        COMPAT_TYPES.put(Types.DOUBLE + "|" + Types.DECIMAL, Types.DOUBLE);
        COMPAT_TYPES.put(Types.DOUBLE + "|" + Types.NUMERIC, Types.DOUBLE);
        
        COMPAT_TYPES.put(Types.DECIMAL + "|" + Types.BIT, Types.DECIMAL);
        COMPAT_TYPES.put(Types.DECIMAL + "|" + Types.TINYINT, Types.DECIMAL);
        COMPAT_TYPES.put(Types.DECIMAL + "|" + Types.SMALLINT, Types.DECIMAL);
        COMPAT_TYPES.put(Types.DECIMAL + "|" + Types.INTEGER, Types.DECIMAL);
        COMPAT_TYPES.put(Types.DECIMAL + "|" + Types.BIGINT, Types.DECIMAL);
        COMPAT_TYPES.put(Types.DECIMAL + "|" + Types.FLOAT, Types.DECIMAL);
        COMPAT_TYPES.put(Types.DECIMAL + "|" + Types.REAL, Types.DECIMAL);
        COMPAT_TYPES.put(Types.DECIMAL + "|" + Types.NUMERIC, Types.DECIMAL);
        
        COMPAT_TYPES.put(Types.REAL + "|" + Types.BIT, Types.REAL);
        COMPAT_TYPES.put(Types.REAL + "|" + Types.TINYINT, Types.REAL);
        COMPAT_TYPES.put(Types.REAL + "|" + Types.SMALLINT, Types.REAL);
        COMPAT_TYPES.put(Types.REAL + "|" + Types.INTEGER, Types.REAL);
        COMPAT_TYPES.put(Types.REAL + "|" + Types.BIGINT, Types.REAL);
        COMPAT_TYPES.put(Types.REAL + "|" + Types.FLOAT, Types.REAL);
        COMPAT_TYPES.put(Types.REAL + "|" + Types.DECIMAL, Types.REAL);
        COMPAT_TYPES.put(Types.REAL + "|" + Types.NUMERIC, Types.REAL);
        
        COMPAT_TYPES.put(Types.FLOAT + "|" + Types.BIT, Types.FLOAT);
        COMPAT_TYPES.put(Types.FLOAT + "|" + Types.TINYINT, Types.FLOAT);
        COMPAT_TYPES.put(Types.FLOAT + "|" + Types.SMALLINT, Types.FLOAT);
        COMPAT_TYPES.put(Types.FLOAT + "|" + Types.INTEGER, Types.FLOAT);
        COMPAT_TYPES.put(Types.FLOAT + "|" + Types.BIGINT, Types.FLOAT);
        COMPAT_TYPES.put(Types.FLOAT + "|" + Types.REAL, Types.FLOAT);
        COMPAT_TYPES.put(Types.FLOAT + "|" + Types.DECIMAL, Types.FLOAT);
        COMPAT_TYPES.put(Types.FLOAT + "|" + Types.NUMERIC, Types.FLOAT);
        
        COMPAT_TYPES.put(Types.BIGINT + "|" + Types.BIT, Types.FLOAT);
        COMPAT_TYPES.put(Types.BIGINT + "|" + Types.TINYINT, Types.FLOAT);
        COMPAT_TYPES.put(Types.BIGINT + "|" + Types.SMALLINT, Types.FLOAT);
        COMPAT_TYPES.put(Types.BIGINT + "|" + Types.INTEGER, Types.FLOAT);
        
        COMPAT_TYPES.put(Types.INTEGER + "|" + Types.BIT, Types.INTEGER);
        COMPAT_TYPES.put(Types.INTEGER + "|" + Types.TINYINT, Types.INTEGER);
        COMPAT_TYPES.put(Types.INTEGER + "|" + Types.SMALLINT, Types.INTEGER);
        
        COMPAT_TYPES.put(Types.SMALLINT + "|" + Types.BIT, Types.SMALLINT);
        COMPAT_TYPES.put(Types.SMALLINT + "|" + Types.TINYINT, Types.SMALLINT);
        
        COMPAT_TYPES.put(Types.TINYINT + "|" + Types.BIT, Types.TINYINT);
        
        COMPAT_TYPES.put(Types.BINARY + "|" + Types.BIT, Types.BINARY);
    }
    
    static
    {
        REPLACEABLE_TYPES.put(Types.BLOB, new int[] {Types.BINARY,
                Types.VARBINARY, Types.LONGVARBINARY, Types.JAVA_OBJECT,
                Types.ARRAY});
        
        REPLACEABLE_TYPES.put(Types.VARBINARY, new int[] {Types.BLOB,
                Types.BINARY, Types.LONGVARBINARY, Types.JAVA_OBJECT,
                Types.ARRAY});
        
        REPLACEABLE_TYPES.put(Types.LONGVARBINARY, new int[] {Types.BLOB,
                Types.VARBINARY, Types.BINARY, Types.JAVA_OBJECT, Types.ARRAY});
        
        REPLACEABLE_TYPES.put(Types.STRUCT, new int[] {Types.STRUCT});
        
        REPLACEABLE_TYPES.put(Types.JAVA_OBJECT,
                new int[] {Types.BLOB, Types.VARBINARY, Types.LONGVARBINARY,
                        Types.BINARY, Types.ARRAY});
        
        REPLACEABLE_TYPES.put(Types.ARRAY, new int[] {Types.BLOB,
                Types.VARBINARY, Types.LONGVARBINARY, Types.BINARY,
                Types.JAVA_OBJECT});
        
        REPLACEABLE_TYPES.put(Types.CLOB, new int[] {Types.LONGVARCHAR,
                SqlUtils.LONGNVARCHAR, SqlUtils.NCLOB, Types.VARCHAR,
                Types.CHAR, SqlUtils.SQLXML});
        
        REPLACEABLE_TYPES.put(Types.LONGVARCHAR, new int[] {Types.CLOB,
                SqlUtils.LONGNVARCHAR, SqlUtils.NCLOB, Types.VARCHAR,
                Types.CHAR, SqlUtils.SQLXML});
        
        REPLACEABLE_TYPES.put(Types.LONGNVARCHAR, new int[] {Types.CLOB,
                Types.LONGVARCHAR, SqlUtils.NCLOB, Types.VARCHAR, Types.CHAR,
                SqlUtils.SQLXML});
        
        REPLACEABLE_TYPES.put(Types.NCLOB, new int[] {Types.CLOB,
                Types.LONGVARCHAR, Types.LONGNVARCHAR, Types.VARCHAR,
                Types.CHAR, SqlUtils.SQLXML});
        
        REPLACEABLE_TYPES.put(SqlUtils.SQLXML, new int[] {Types.CLOB,
                Types.LONGVARCHAR, Types.LONGNVARCHAR, Types.VARCHAR,
                Types.CHAR, Types.NCLOB});
        
        REPLACEABLE_TYPES.put(Types.CHAR, new int[] {Types.VARCHAR,
                SqlUtils.NCHAR, Types.LONGVARCHAR, SqlUtils.NVARCHAR,
                SqlUtils.LONGNVARCHAR});
        
        REPLACEABLE_TYPES.put(Types.VARCHAR, new int[] {Types.CHAR,
                Types.LONGVARCHAR, SqlUtils.NCHAR, SqlUtils.NVARCHAR,
                SqlUtils.LONGNVARCHAR});
        
        REPLACEABLE_TYPES.put(SqlUtils.NCHAR, new int[] {Types.CHAR,
                Types.LONGVARCHAR, SqlUtils.LONGNVARCHAR, SqlUtils.NVARCHAR,
                Types.VARCHAR,});
        
        REPLACEABLE_TYPES.put(SqlUtils.NVARCHAR, new int[] {Types.LONGVARCHAR,
                SqlUtils.LONGNVARCHAR, SqlUtils.NCHAR, Types.VARCHAR,
                Types.CHAR});
        
        REPLACEABLE_TYPES.put(Types.DATE, new int[] {Types.TIMESTAMP,
                Types.TIME});
        
        REPLACEABLE_TYPES.put(Types.TIME, new int[] {Types.TIMESTAMP,
                Types.DATE});
        
        REPLACEABLE_TYPES.put(Types.TIMESTAMP, new int[] {Types.DATE,
                Types.TIME});
        
        REPLACEABLE_TYPES.put(Types.NUMERIC, new int[] {Types.DOUBLE,
                Types.DECIMAL, Types.REAL, Types.FLOAT, Types.BIGINT,
                Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.BIT});
        
        REPLACEABLE_TYPES.put(Types.DOUBLE, new int[] {Types.NUMERIC,
                Types.DECIMAL, Types.REAL, Types.FLOAT, Types.BIGINT,
                Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.BIT});
        
        REPLACEABLE_TYPES.put(Types.REAL, new int[] {Types.NUMERIC,
                Types.DOUBLE, Types.DECIMAL, Types.FLOAT, Types.BIGINT,
                Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.BIT});
        
        REPLACEABLE_TYPES.put(Types.FLOAT, new int[] {Types.NUMERIC,
                Types.DOUBLE, Types.DECIMAL, Types.REAL, Types.BIGINT,
                Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.BIT});
        
        REPLACEABLE_TYPES.put(Types.BIGINT, new int[] {Types.NUMERIC,
                Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.BIT,
                Types.DOUBLE, Types.DECIMAL, Types.FLOAT, Types.REAL});
        
        REPLACEABLE_TYPES.put(Types.INTEGER, new int[] {Types.NUMERIC,
                Types.BIGINT, Types.SMALLINT, Types.TINYINT, Types.BIT,
                Types.DOUBLE, Types.DECIMAL, Types.FLOAT, Types.REAL});
        
        REPLACEABLE_TYPES.put(Types.SMALLINT, new int[] {Types.NUMERIC,
                Types.INTEGER, Types.BIGINT, Types.TINYINT, Types.BIT,
                Types.DOUBLE, Types.DECIMAL, Types.FLOAT, Types.REAL});
        
        REPLACEABLE_TYPES.put(Types.TINYINT, new int[] {Types.NUMERIC,
                Types.SMALLINT, Types.INTEGER, Types.BIT, Types.BIGINT,
                Types.DOUBLE, Types.DECIMAL, Types.FLOAT, Types.REAL});
        
        REPLACEABLE_TYPES.put(Types.BIT, new int[] {Types.NUMERIC,
                Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT,
                Types.DOUBLE, Types.DECIMAL, Types.FLOAT, Types.REAL});
        
        REPLACEABLE_TYPES.put(Types.BINARY, new int[] {Types.NUMERIC,
                Types.BIT, Types.TINYINT, Types.SMALLINT, Types.INTEGER,
                Types.BIGINT, Types.DOUBLE, Types.DECIMAL, Types.FLOAT,
                Types.REAL});
    }
    
    /** The TYPES. */
    private static Map<Integer, String> TYPES = new HashMap<Integer, String>();
    
    static
    {
        TYPES.put(Types.BIT, "BIT");
        TYPES.put(Types.TINYINT, "TINYINT");
        TYPES.put(Types.SMALLINT, "SMALLINT");
        TYPES.put(Types.INTEGER, "INTEGER");
        TYPES.put(Types.BIGINT, "BIGINT");
        TYPES.put(Types.FLOAT, "FLOAT");
        TYPES.put(Types.REAL, "REAL");
        TYPES.put(Types.DOUBLE, "DOUBLE");
        TYPES.put(Types.NUMERIC, "NUMERIC");
        TYPES.put(Types.DECIMAL, "DECIMAL");
        TYPES.put(Types.CHAR, "CHAR");
        TYPES.put(Types.VARCHAR, "VARCHAR");
        TYPES.put(Types.LONGVARCHAR, "LONGVARCHAR");
        TYPES.put(Types.DATE, "DATE");
        TYPES.put(Types.TIME, "TIME");
        TYPES.put(Types.TIMESTAMP, "TIMESTAMP");
        TYPES.put(Types.BINARY, "BINARY");
        TYPES.put(Types.VARBINARY, "VARBINARY");
        TYPES.put(Types.LONGVARBINARY, "LONGVARBINARY");
        TYPES.put(Types.NULL, "NULL");
        TYPES.put(Types.OTHER, "OTHER");
        TYPES.put(Types.JAVA_OBJECT, "JAVA OBJECT");
        TYPES.put(Types.DISTINCT, "DISTINCT");
        TYPES.put(Types.STRUCT, "STRUCT");
        TYPES.put(Types.ARRAY, "ARRAY");
        TYPES.put(Types.BLOB, "BLOB");
        TYPES.put(Types.CLOB, "CLOB");
        TYPES.put(Types.REF, "REF");
        TYPES.put(Types.DATALINK, "DATALINK");
        TYPES.put(Types.BOOLEAN, "BOOLEAN");
        TYPES.put(Types.ROWID, "ROWID");
        TYPES.put(Types.NCHAR, "NCHAR");
        TYPES.put(Types.NVARCHAR, "NVARCHAR");
        TYPES.put(Types.LONGNVARCHAR, "LONGNVARCHAR");
        TYPES.put(Types.NCLOB, "NCLOB");
        TYPES.put(Types.SQLXML, "SQLXML");
    }
    
    /** The TYPE NAMES. */
    private static Map<String, Integer> TYPE_NAMES = new TreeMap<String, Integer>(
            String.CASE_INSENSITIVE_ORDER);
    
    static
    {
        TYPE_NAMES.put("BIT", Types.BIT);
        TYPE_NAMES.put("TINYINT", Types.TINYINT);
        TYPE_NAMES.put("SMALLINT", Types.SMALLINT);
        TYPE_NAMES.put("INTEGER", Types.INTEGER);
        TYPE_NAMES.put("INT", Types.INTEGER);
        TYPE_NAMES.put("BIGINT", Types.BIGINT);
        TYPE_NAMES.put("FLOAT", Types.FLOAT);
        TYPE_NAMES.put("REAL", Types.REAL);
        TYPE_NAMES.put("DOUBLE", Types.DOUBLE);
        TYPE_NAMES.put("NUMERIC", Types.NUMERIC);
        TYPE_NAMES.put("NUMBER", Types.NUMERIC);
        TYPE_NAMES.put("DECIMAL", Types.DECIMAL);
        TYPE_NAMES.put("CHAR", Types.CHAR);
        TYPE_NAMES.put("VARCHAR", Types.VARCHAR);
        TYPE_NAMES.put("LONGVARCHAR", Types.LONGVARCHAR);
        TYPE_NAMES.put("STRING", Types.VARCHAR);
        TYPE_NAMES.put("DATE", Types.DATE);
        TYPE_NAMES.put("TIME", Types.TIME);
        TYPE_NAMES.put("TIMESTAMP", Types.TIMESTAMP);
        TYPE_NAMES.put("BINARY", Types.BINARY);
        TYPE_NAMES.put("VARBINARY", Types.VARBINARY);
        TYPE_NAMES.put("LONGVARBINARY", Types.LONGVARBINARY);
        TYPE_NAMES.put("NULL", Types.NULL);
        TYPE_NAMES.put("OTHER", Types.OTHER);
        TYPE_NAMES.put("JAVA OBJECT", Types.JAVA_OBJECT);
        TYPE_NAMES.put("DISTINCT", Types.DISTINCT);
        TYPE_NAMES.put("STRUCT", Types.STRUCT);
        TYPE_NAMES.put("ARRAY", Types.ARRAY);
        TYPE_NAMES.put("BLOB", Types.BLOB);
        TYPE_NAMES.put("CLOB", Types.CLOB);
        TYPE_NAMES.put("REF", Types.REF);
        TYPE_NAMES.put("DATALINK", Types.DATALINK);
        TYPE_NAMES.put("BOOLEAN", Types.BOOLEAN);
        TYPE_NAMES.put("ROWID", Types.ROWID);
        TYPE_NAMES.put("NCHAR", Types.NCHAR);
        TYPE_NAMES.put("NVARCHAR", Types.NVARCHAR);
        TYPE_NAMES.put("LONGNVARCHAR", Types.LONGNVARCHAR);
        TYPE_NAMES.put("NCLOB", Types.NCLOB);
        TYPE_NAMES.put("SQLXML", Types.SQLXML);
    }
    
    /**
     * Adds the bind variable to the <code>bindVars</code> using given name,
     * value and array of names <code>usingVars</code>.
     * 
     * @param usingVars
     *            the array of variable names. Similar to sql using clause.
     * @param name
     *            the name of the variable
     * @param value
     *            the value of the variable
     * @param bindVars
     *            the bind variables
     * @return true, if variable is added
     */
    public static boolean addBindVar(String[] usingVars, String name,
            Object value, Map<String, Object> bindVars)
    {
        boolean added = false;
        
        if (usingVars == null)
            return false;
        
        for (int i = 0; i < usingVars.length; i++)
        {
            String var = usingVars[i];
            
            if (!Utils.isNothing(var))
            {
                var = var.trim().toUpperCase();
                
                if (var.equals(name.trim().toUpperCase()))
                {
                    bindVars.put(String.valueOf(i), value);
                    added = true;
                }
            }
        }
        
        return added;
    }
    
    /**
     * Checks if typeName is already formatted type.
     * 
     * <p>
     * Example:
     * <p>
     * varchar(20) -> true
     * <p>
     * varchar - > false
     * 
     * @param typeName
     *            the type name
     * @return true, if typeName is already formated type
     */
    public static boolean alreadyFormatedType(String typeName)
    {
        if (typeName == null)
            return true;
        
        int start = typeName.indexOf("(");
        int end = typeName.indexOf(")");
        
        if (start >= 0 && end > start)
            return !Utils.isNothing(typeName.substring(start + 1, end).trim());
        else
            return true;
    }
    
    /**
     * Converts owner.name to name.
     * 
     * <p>
     * Example:
     * <p>
     * abc.xyz - > xyz
     * <p>
     * abc.mmm.xyz - > xyz
     * 
     * @param baseName
     *            the base name
     * @return the string
     */
    public static String baseName2Name(String baseName)
    {
        if (Utils.isNothing(baseName))
            return null;
        
        int index = baseName.lastIndexOf(".");
        
        if (index < 0)
            return baseName;
        
        return baseName.substring(index + 1);
    }
    
    /**
     * Close allocated resources for the passed SQL Statement and ResultSet
     * objects.
     * 
     * @param statement
     *            A SQL Statement to close.
     * @param rs
     *            A SQL ResultSet to close.
     * @param caller
     *            The object that controls the passed SQL objects
     */
    public static void cleanUpSQLData(Statement statement, ResultSet rs,
            Object caller)
    {
        try
        {
            if (rs != null)
            {
                rs.close();
            }
        }
        catch (Exception sqe)
        {
            Logger.log(
                    Logger.WARNING,
                    rs,
                    EtlResource.ERROR_CLOSING_RESULT_SET.getValue()
                            + sqe.getMessage());
        }
        try
        {
            if (statement != null)
            {
                statement.close();
            }
        }
        catch (Exception sqe)
        {
            Logger.log(Logger.WARNING, statement,
                    EtlResource.ERROR_CLOSING_PREPARED_STATEMENT.getValue()
                            + sqe.getMessage());
        }
    }
    
    /**
     * Converts source data type into destination data type using given source
     * and dest field definitions.
     * 
     * @param source
     *            the source field
     * @param dest
     *            the destination field
     * @return the native formatted data type
     */
    public static String convertDataType(FieldDef source, FieldDef dest)
    {
        if (source == null)
            return null;
        
        if (dest == null)
            return source.getNativeDataType();
        
        String range = null;
        
        if (dest.getBestMatch() != null)
            dest = dest.getBestMatch();
        
        if (dest.isChar() && dest.hasParams())
        {
            range = getTypeRange(
                    source.getNativeDataType(),
                    dest.getPrecision() > 0 ? dest.getPrecision() : source
                            .getPrecision(), -1);
        }
        else if (isNumber(dest.getSqlDataType()) && dest.hasParams())
        {
            range = SqlUtils.getTypeRange(source.getNativeDataType(),
                    dest.getPrecision(), dest.getScale());
        }
        else if (dest.hasParams())
        {
            range = getTypeRange(source.getNativeDataType(),
                    dest.getPrecision(), dest.getScale());
        }
        
        if (range != null)
        {
            if (!dest.getNativeDataType().matches(".*?\\(.*?\\).*?"))
                return dest.getNativeDataType() + range;
            else
                return Utils.removeWhiteSpace(dest.getNativeDataType()
                        .replaceAll("\\(.*?\\)", range));
        }
        
        return dest.getNativeDataType();
    }
    
    /**
     * Checks if data type has size.
     * 
     * <p>
     * Example:
     * <p>
     * fType = Types.VARCHAR -> true
     * <p>
     * fType = Types.INTEGEr -> false
     * 
     * @param fType
     *            the SQL data type
     * @return true, if successful
     */
    public static boolean dataTypeHasSize(int fType)
    {
        return (fType == Types.NUMERIC || fType == Types.DECIMAL
                || fType == Types.FLOAT || fType == Types.REAL
                || fType == Types.DOUBLE || fType == Types.CHAR
                || fType == Types.VARCHAR || fType == Types.LONGVARCHAR
                || fType == SqlUtils.NCHAR || fType == SqlUtils.NVARCHAR);
    }
    
    /**
     * Converts date to string using either default DATE FORMAT or format
     * defined in the <code>params</code>. The DATE_FORMAT_PROP is used to look
     * up format in the <code>params</code>.
     * 
     * @param fieldValue
     *            the field value
     * @param params
     *            the parameters
     * @return the string
     */
    public static String date2Str(Object fieldValue, Map<String, String> params)
    {
        return internalDate2Str(fieldValue, params, DATE_FORMAT_PROP,
                DataSet.DATA_SET_DATE_TIME_FORMAT);
    }
    
    /**
     * Converts date+time to string using either default DATE_TIME FORMAT or
     * format defined in the <code>params</code>. The DATE_TIME_FORMAT_PROP is
     * used to look up format in the <code>params</code>.
     * 
     * @param fieldValue
     *            the field value
     * @param params
     *            the parameters
     * @return the string
     */
    public static String dateTime2Str(Object fieldValue,
            Map<String, String> params)
    {
        return internalDate2Str(fieldValue, params, DATE_TIME_FORMAT_PROP,
                DataSet.DATA_SET_DATE_TIME_FORMAT);
    }
    
    /**
     * Converts decimal to string. Depending on value of the DECIMAL_AS_INT_PROP
     * property the trailing zeros can be truncated. The default value of the '
     * DECIMAL_AS_INT_PROP is true.
     * 
     * @param value
     *            the value
     * @param params
     *            the parameters
     * @return the string
     */
    public static String decimal2Str(Object value, Map<String, String> params)
    {
        if (value instanceof Number)
        {
            boolean decimalAsInt = Utils.str2Boolean(
                    Utils.getParamFromMap(DECIMAL_AS_INT_PROP, params, "true"),
                    true);
            
            if (decimalAsInt && !Utils.isDecimal((Number)value))
                return Long.valueOf(((Number)value).longValue()).toString();
            else
                return value.toString();
        }
        else
            return value.toString();
    }
    
    /**
     * Executes SQL script for all rows of the data set if its not empty or just
     * once if it is. ";" used as a delimiter for the sql statements. Each SQL
     * statement gets prepared and parameters get assigned using
     * <code>bindVariables</code> and <code>using</code>.
     * <code>onException</code> serves as an exceptions handler.
     *
     * @param con the conection
     * @param script The sql script.
     * @param driver the driver
     * @param dataSet the data set
     * @param onException The exceptions handler
     * @param bindVariables The bind variables
     * @param using The using for bind variables
     * @param commit the commit
     * @throws Exception in case of any error
     */
    public static void executeScript(Connection con, String script,
            Driver driver, DataSet dataSet, OnException onException,
            Map<String, Object> bindVariables, String using, boolean commit)
        throws Exception
    {
        if (script == null)
            return;
        
        int action = onException.getOnExceptionAction();
        String exception = onException.getExceptionMask();
        int ret = 0;
        
        SqlParser sqlParser = (SqlParser)ObjectFactory.instance().get(
                SqlParser.class.getName(), GenericSqlParser.class.getName(),
                true);
        
        String[] stmts = sqlParser.split(script);
        
        try
        {
            for (String sql : stmts)
            {
                
                if (!Utils.isNothing(sql))
                {
                    if (dataSet.getRecordCount() == 0)
                    {
                        ret = executeSql(con, sql, action, exception,
                                bindVariables, driver);
                        
                        if (ret == OnException.ON_EXCEPTION_CONTINUE)
                            continue;
                        else if (ret == OnException.ON_EXCEPTION_IGNORE)
                            return;
                    }
                    else
                        for (int i = 0; i < dataSet.getRecordCount(); i++)
                        {
                            ret = executeSql(
                                    con,
                                    prepareSql(sql, dataSet, i, driver,
                                            bindVariables, using), action,
                                    exception, bindVariables, driver);
                            
                            if (ret == OnException.ON_EXCEPTION_CONTINUE)
                                continue;
                            else if (ret == OnException.ON_EXCEPTION_IGNORE)
                                break;
                        }
                    
                }
            }
            
            if (commit)
                con.commit();
        }
        catch (Exception ex)
        {
            con.rollback();
            
            throw ex;
        }
    }
    
    /**
     * Prepares sql, set bind variables using given <code>bindVars</code> and
     * finally executes sql. In case of exception returns appropriate exception
     * handler code.
     *
     * @param con the connection
     * @param sql the sql to execute
     * @param action The action. Possible actions are:
     * <code>ON_EXCEPTION_RAISE, ON_EXCEPTION_IGNORE,ON_EXCEPTION_CONTINUE,
     * ON_EXCEPTION_MERGE</code>
     * @param exception The string which can be used to mask exceptions.
     * @param bindVars The bind variables
     * @param driver the driver
     * @return <code>0<code> if there is no exception, otherwise one of the ON_EXCEPTION_ values
     * @throws Exception in case of any error
     */
    private static int executeSql(Connection con, String sql, int action,
            String exception, Map<String, Object> bindVars, Driver driver)
        throws Exception
    {
        PreparedStatement preparedStatement = null;
        
        Savepoint svp = null;
        
        try
        {
            preparedStatement = con.prepareStatement(sql);
            
            for (int i = 0; i < bindVars.size(); i++)
            {
                Object bindVar = bindVars.get(String.valueOf(i));
                
                setBindVar(preparedStatement, bindVar, i + 1);
            }
            
            if (driver != null && driver.requiresRollbackAfterSqlError())
                svp = getSavepoint(con);
            
            preparedStatement.execute();
        }
        catch (Exception ex)
        {
            if (svp != null)
                con.rollback(svp);
            
            if (action == OnException.ON_EXCEPTION_RAISE)
                throw ex;
            
            String message = ex.getMessage();
            if (!Utils.isNothing(exception)
                    && !Utils.isNothing(message)
                    && message.toUpperCase().indexOf(exception.toUpperCase()) < 0)
                throw ex;
            
            if (action == OnException.ON_EXCEPTION_CONTINUE)
                return OnException.ON_EXCEPTION_CONTINUE;
            
            if (action == OnException.ON_EXCEPTION_IGNORE)
                return OnException.ON_EXCEPTION_IGNORE;
        }
        finally
        {
            cleanUpSQLData(preparedStatement, null, null);
        }
        
        return 0;
    }
    
    /**
     * Prepares sql, set bind variables using given <code>params</code> and
     * finally executes sql. There can be multiple sql statements separated by
     * ";". In this case statements executed one by one.
     * 
     * @param con
     *            the con
     * @param sql
     *            the sql
     * @param params
     *            the parameters
     * @param driver
     *            the driver
     * @throws Exception
     *             in case of any error
     */
    public static void executeSql(Connection con, String sql,
            LinkedHashMap<String, Object> params, Driver driver)
        throws Exception
    {
        if (Utils.isNothing(sql))
            return;
        
        PreparedStatement st = null;
        
        SqlParser sqlParser = (SqlParser)ObjectFactory.instance().get(
                SqlParser.class.getName(), GenericSqlParser.class.getName(),
                true);
        
        String[] stmts = sqlParser.split(sql);
        
        for (String sqlToRun : stmts)
        {
            Map<String, List<Integer>> paramMap = null;
            
            if (params != null && params.size() > 0)
            {
                paramMap = new HashMap<String, List<Integer>>();
                
                sqlToRun = sqlParser.parseParams(sqlToRun, null, paramMap,
                        null, driver);
            }
            
            if (Utils.isNothing(sqlToRun))
                continue;
            
            try
            {
                st = con.prepareStatement(sqlToRun);
                
                // sql has bind variables
                if (paramMap != null && paramMap.size() > 0)
                    sqlParser.setBindVariables(st, params, paramMap, null,
                            driver);
                
                st.execute();
            }
            finally
            {
                cleanUpSQLData(st, null, null);
                
                st = null;
            }
        }
    }
    
    /**
     * Checks if field must have parameters.
     * 
     * @param fieldType
     *            the field type {@link java.sql.Types}
     * @return true, if successful
     */
    public static boolean fieldHasParams(int fieldType)
    {
        return fieldType == Types.VARCHAR || fieldType == Types.CHAR
                || fieldType == Types.NVARCHAR || fieldType == Types.NCHAR;
    }
    
    /**
     * Executes {@link Driver#filter(String)} if field is a flavor of char or
     * clob.
     * 
     * @param driver
     *            the driver
     * @param value
     *            the value
     * @param fieldType
     *            the field type {@link java.sql.Types}
     * @return the string
     */
    public static String filter(Driver driver, String value, int fieldType)
    {
        if (isChar(fieldType) || isClob(fieldType))
            return driver.filter(value);
        else
            return value;
    }
    
    /**
     * Converts full name to base name.
     * 
     * <p>
     * Example:
     * <p>
     * abc:xyz -> xyz
     * <p>
     * abc:mmm:xyz -> xyz
     * 
     * @param fullName
     *            the full name
     * @return the string
     */
    public static String fullName2BaseName(String fullName)
    {
        if (Utils.isNothing(fullName))
            return null;
        
        int index = fullName.indexOf(":");
        
        if (index < 0)
            return null;
        
        return fullName.substring(index + 1);
    }
    
    /**
     * Converts full name to name.
     * 
     * <p>
     * Example:
     * <p>
     * Java DB Test:APP.DATA_SOURCE -> DATA_SOURCE
     * 
     * @param fullName
     *            the full name
     * @return the string
     */
    public static String fullName2Name(String fullName)
    {
        if (Utils.isNothing(fullName))
            return null;
        
        String baseName = fullName2BaseName(fullName);
        
        if (Utils.isNothing(baseName))
            return null;
        
        int index = baseName.lastIndexOf(".");
        
        if (index < 0)
            return null;
        
        return baseName.substring(index + 1);
    }
    
    /**
     * Converts full name to node name.
     * 
     * <p>
     * Example:
     * <p>
     * Excel Test:C:/projects/toolsverse/java/data-test/test.table1$ -> Excel
     * Test
     * 
     * @param fullName
     *            the full name
     * @return the string
     */
    public static String fullName2NodeName(String fullName)
    {
        if (Utils.isNothing(fullName))
            return null;
        
        int index = fullName.indexOf(":");
        
        if (index < 0)
            return fullName;
        
        return fullName.substring(0, index);
    }
    
    /**
     * Converts full name to owner name.
     * 
     * <p>
     * Example:
     * <p>
     * Excel Test:C:/projects/toolsverse/java/data-test/test.table1$ ->
     * C:/projects/toolsverse/java/data-test/test
     * 
     * @param fullName
     *            the full name
     * @return the string
     */
    public static String fullName2OwnerName(String fullName)
    {
        if (Utils.isNothing(fullName))
            return null;
        
        String baseName = fullName2BaseName(fullName);
        
        if (Utils.isNothing(baseName))
            return null;
        
        int index = baseName.lastIndexOf(".");
        
        if (index < 0)
            return baseName;
        
        return baseName.substring(0, index);
    }
    
    /**
     * Gets part of the sql statement after "from" clause.
     * 
     * <p>
     * Example:
     * <p>
     * seLECt a,B,c,D fROm abc WHERE a=b and c is null order by xxx ->abc WHERE
     * a=b and c is null order by xxx
     * 
     * @param sql
     *            the sql
     * @return the string
     */
    public static String getAfterFromFromSelect(String sql)
    {
        if (Utils.isNothing(sql))
            return sql;
        
        String str = sql.trim();
        
        String[] tokens = str.split("(?i)from");
        
        if (tokens.length < 2)
            return "";
        
        return tokens[1].trim();
    }
    
    /**
     * Gets part of the sql statement after "where" clause.
     * 
     * <p>
     * Example:
     * <p>
     * select a, b,c, d from abc where a=b and c is null ->a=b and c is null
     * 
     * @param sql
     *            the sql
     * @return the string
     */
    public static String getAfterWhereFromSelect(String sql)
    {
        if (Utils.isNothing(sql))
            return sql;
        
        String str = sql.trim();
        
        String[] tokens = str.split("(?i)where");
        
        if (tokens.length < 2)
            return "";
        
        return tokens[1].trim();
    }
    
    /**
     * Reads the blob field from the result set.
     * 
     * @param rs
     *            the result set
     * @param pos
     *            the position of the field
     * @return the blob
     * @throws Exception
     *             in case of any error
     */
    public static Object getBlob(ResultSet rs, int pos)
        throws Exception
    {
        try
        {
            InputStream blobStream = getBlobInputStreamFromResultSet(rs, pos);
            
            if (blobStream == null)
                return null;
            
            return Utils.readBytesFromInputStream(blobStream, true);
        }
        catch (Exception ex)
        {
            return rs.getObject(pos);
        }
    }
    
    /**
     * Gets the blob as an input stream from the result set.
     * 
     * @param rs
     *            the result set
     * @param columnNum
     *            the column number
     * @return the blob as an input stream
     * @throws SQLException
     *             the SQL exception
     */
    public static InputStream getBlobInputStreamFromResultSet(ResultSet rs,
            int columnNum)
        throws SQLException
    {
        Object obj = rs.getBlob(columnNum);
        
        if (!(obj instanceof java.sql.Blob))
            return null;
        
        java.sql.Blob blob = (java.sql.Blob)obj;
        
        return blob.getBinaryStream();
    }
    
    /**
     * Getter an output stream to write data into the passed Blob.
     * 
     * @param blob
     *            Blob
     * @return the blob output stream
     * @throws SQLException
     *             the sQL exception
     */
    public static OutputStream getBlobOutputStream(Blob blob)
        throws SQLException
    {
        OutputStream rtn = null;
        
        try
        {
            Method method = blob.getClass().getMethod(BLOB_OUTPUT_METHOD,
                    BLOB_OUTPUT_PARAM_TYPES);
            
            rtn = (OutputStream)method.invoke(blob, BLOB_OUTPUT_PARAMS);
        }
        catch (NoSuchMethodException e)
        {
            rtn = blob.setBinaryStream(0);
        }
        catch (InvocationTargetException e)
        {
            if (e.getCause() instanceof SQLException)
                throw ((SQLException)e.getCause());
            else
            {
                throw new IllegalStateException(
                        Resource.ERROR_ACCESSING_BLOB_STREAM.getValue());
            }
        }
        catch (IllegalAccessException e)
        {
            throw new IllegalStateException(
                    Resource.ERROR_ACCESSING_BLOB_STREAM.getValue());
        }
        
        return rtn;
    }
    
    /**
     * Gets the blob output stream from the result set.
     * 
     * @param rs
     *            the result set
     * @param columnNum
     *            the column number
     * @return the blob output stream
     * @throws SQLException
     *             the SQL exception
     */
    public static OutputStream getBlobOutputStreamFromResultSet(ResultSet rs,
            int columnNum)
        throws SQLException
    {
        Object obj = rs.getBlob(columnNum);
        
        if (!(obj instanceof java.sql.Blob))
            return null;
        
        java.sql.Blob blob = (java.sql.Blob)obj;
        
        return getBlobOutputStream(blob);
    }
    
    /**
     * Reads the clob from the result set.
     * 
     * @param rs
     *            the result set
     * @param pos
     *            the position of the clob field
     * @return the clob
     * @throws Exception
     *             in case of any error
     */
    public static Object getClob(ResultSet rs, int pos)
        throws Exception
    {
        try
        {
            Object obj = rs.getClob(pos);
            
            if (!(obj instanceof java.sql.Clob))
                return null;
            
            java.sql.Clob clob = (java.sql.Clob)obj;
            
            return Utils.readStringFromInputStream(clob.getAsciiStream());
        }
        catch (Exception ex)
        {
            return rs.getObject(pos);
        }
    }
    
    /**
     * Calculates column name using given name.
     * 
     * <p>
     * Example:
     * 
     * <pre>
     * Map<String, Integer> cols = new HashMap<String, Integer>()
     * SqlUtils.getColName(cols, "col") -> col
     * SqlUtils.getColName(cols, "col") -> col1
     * SqlUtils.getColName(cols, "col") -> col2
     * </pre>
     * 
     * @param cols
     *            the columns
     * @param colName
     *            the column name
     * @return the new column nme
     */
    public static String getColName(Map<String, Integer> cols, String colName)
    {
        Integer index = cols.get(colName);
        
        if (index == null)
        {
            cols.put(colName, new Integer(0));
            
            return colName;
        }
        else
        {
            int value = index.intValue() + 1;
            cols.put(colName, new Integer(value));
            
            return colName + value;
        }
    }
    
    /**
     * Gets the field definition.
     * 
     * <p>
     * Example:
     * <p>
     * SqlUtils.getFieldAsText(driver, "test", "varchar", 100, null, "Yes",
     * Types.VARCHAR) -> test varchar(100)
     * <p>
     * SqlUtils.getFieldAsText(driver, "test", "number", 18, 5, true,
     * Types.NUMERIC) -> test number(18,5)
     * 
     * @param driver
     *            the driver
     * @param name
     *            the field name
     * @param fType
     *            the field type
     * @param size
     *            the size
     * @param decimal
     *            the number of decimals
     * @param nullable
     *            the nullable flag
     * @param javaType
     *            the {@link java.sql.Types}
     * @return the field as text
     */
    public static String getFieldAsText(Driver driver, String name,
            String fType, Object size, Object decimal, Object nullable,
            int javaType)
    {
        String[] typeTokens = fType.split(" ");
        
        String field = name + " " + typeTokens[0];
        
        if (!Utils.isNothing(size) && driver.typeHasSize(javaType))
        {
            field = name2RightCase(driver, field) + "(" + size.toString();
            
            if (!Utils.isNothing(decimal))
            {
                if ((decimal instanceof Number && ((Number)decimal).intValue() != 0)
                        || ((decimal instanceof String) && Utils.str2Number(
                                (String)decimal, 0).intValue() != 0))
                {
                    int pr = Integer.parseInt(decimal.toString());
                    
                    if (pr != driver.getWrongScale())
                        field = field + "," + decimal.toString();
                }
            }
            
            field = field + ")";
        }
        
        if (typeTokens.length > 1)
            for (int i = 1; i < typeTokens.length; i++)
            {
                field = field + " " + typeTokens[i];
            }
        
        if ((nullable instanceof Boolean && !(Boolean)nullable)
                || (nullable instanceof String && !Utils.str2Boolean(
                        nullable.toString(), false)))
            field = field + SqlUtils.name2RightCase(driver, " NOT NULL");
        
        return field;
        
    }
    
    /**
     * Gets the fields from "select" sql statement.
     * 
     * <p>
     * Example:
     * <p>
     * select a,b,c,d from abc -> a,b,c,d
     * 
     * @param sql
     *            the sql
     * @return the fields
     */
    public static String getFieldsFromSelect(String sql)
    {
        if (Utils.isNothing(sql))
            return sql;
        
        String str = sql.trim();
        
        String[] tokens = str.split("(?i)select");
        
        if (tokens.length < 2)
            return "";
        
        tokens = tokens[1].split("(?i)from");
        
        if (tokens.length < 2)
            return "";
        
        return tokens[0].trim();
    }
    
    /**
     * Gets the fields definitions for all fields in the data set. It can be
     * later used in the insert sql statement.
     * 
     * @param dataSet
     *            the data set
     * @param driver
     *            the driver
     * @param key
     *            the key field
     * @param fieldsRepository
     *            the fields repository
     * @return the fields definitions
     *         {@link com.toolsverse.etl.common.FieldsRepository}
     */
    public static String getFieldsSql(DataSet dataSet, Driver driver,
            String key, FieldsRepository fieldsRepository)
    {
        int count = dataSet.getFieldCount();
        String sql = "";
        
        for (int i = 0; i < count; i++)
        {
            FieldDef fieldDef = dataSet.getFieldDef(i);
            
            if (!fieldDef.isVisible())
                continue;
            
            String field = fieldDef.getName();
            
            field = name2RightCase(driver, field)
                    + "  "
                    + driver.getType(fieldDef, key, fieldsRepository)
                    + " "
                    + ((fieldDef.isNullable() || !driver.supportsNotNullable()) ? driver
                            .getDefaultNull() : "not null");
            
            if (i < count - 1)
                field = field + ",";
            
            sql = sql + field + "\n";
        }
        
        if (sql.lastIndexOf(",") == sql.length() - 2)
        {
            sql = sql.substring(0, sql.lastIndexOf(",")) + "\n";
        }
        
        return sql;
    }
    
    /**
     * Gets the field type.
     * 
     * <p>
     * 
     * <pre>
     * if (isCompatible(existingType, newType))
     *     return existingType;
     * if (isCompatible(newType, existingType))
     *     return newType;
     * </pre>
     * 
     * @param newType
     *            the new type {@link java.sql.Types}
     * @param existingType
     *            the existing type {@link java.sql.Types}
     * @param hasNotNullValue
     *            the "has not null value" flag
     * @return the field type {@link java.sql.Types}
     */
    public static int getFieldType(int newType, int existingType,
            boolean hasNotNullValue)
    {
        if (existingType < 0 || !hasNotNullValue)
            return newType;
        
        if (isCompatible(existingType, newType))
            return existingType;
        
        if (isCompatible(newType, existingType))
            return newType;
        
        return Types.VARCHAR;
    }
    
    /**
     * Gets the native data type.
     * 
     * @param fieldDef
     *            the field
     * @return the native data type
     */
    public static String getFullNativeType(FieldDef fieldDef)
    {
        if (fieldDef == null)
            return null;
        
        String range = null;
        
        if (fieldDef.isChar() && fieldDef.hasParams())
        {
            range = getTypeRange(fieldDef.getNativeDataType(),
                    fieldDef.getPrecision(), -1);
        }
        else if (isNumber(fieldDef.getSqlDataType()) && fieldDef.hasParams())
        {
            range = getTypeRange(fieldDef.getNativeDataType(),
                    fieldDef.getPrecision(), fieldDef.getScale());
            
        }
        else if (fieldDef.hasParams())
        {
            range = getTypeRange(fieldDef.getNativeDataType(),
                    fieldDef.getPrecision(), -1);
        }
        
        if (range != null)
        {
            if (!fieldDef.getNativeDataType().matches(".*?\\(.*?\\).*?"))
                return fieldDef.getNativeDataType() + range;
            else
                return Utils.removeWhiteSpace(fieldDef.getNativeDataType()
                        .replaceAll("\\(.*?\\)", range));
            
        }
        
        return fieldDef.getNativeDataType();
    }
    
    /**
     * Gets the type name without scale and precision.
     * 
     * <p>
     * Example:
     * <p>
     * VARCHAR(100) --> VARCHAR
     * <p>
     * NUMBER(10, 20) --> NUMBER
     * <p>
     * INTEGER --> INTEGER
     * 
     * @param typeName
     *            the type name
     * @return the type name without scale and precision
     */
    public static String getJustTypeName(String typeName)
    {
        if (typeName == null)
            return null;
        
        int start = typeName.indexOf("(");
        int end = typeName.indexOf(")");
        
        if (start >= 0 && end > start)
            return typeName.substring(0, start).trim();
        else
            return typeName;
        
    }
    
    /**
     * Gets the type (key) and value (value) from the given text. The type is
     * calculated based on the type of the object returned by
     * Utils.str2Number(text, null).
     * 
     * @param text
     *            the text
     * @return the field type and value
     */
    public static TypedKeyValue<Integer, Number> getNumberTypeAndValue(
            String text)
    {
        if (text == null)
            return null;
        
        Number value = Utils.str2Number(text, null);
        
        if (value == null)
            return null;
        
        int ftype = Types.NUMERIC;
        
        if (value instanceof Long)
            ftype = Types.BIGINT;
        else if (value instanceof Integer)
            ftype = Types.INTEGER;
        else if (value instanceof Float)
            ftype = Types.FLOAT;
        if (value instanceof Double)
            ftype = Types.DOUBLE;
        
        return new TypedKeyValue<Integer, Number>(ftype, value);
    }
    
    /**
     * Gets the array of types which can replace given type.
     * 
     * @param type
     *            the type {@link java.sql.Types}
     * @return the replaceable types
     */
    public static int[] getReplaceableTypes(int type)
    {
        return REPLACEABLE_TYPES.get(type);
    }
    
    /**
     * 
     * Sets the savepoint.
     * 
     * @param con
     *            the connection
     * @return the savepoint
     */
    public static Savepoint getSavepoint(Connection con)
    {
        try
        {
            return con.setSavepoint();
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * Gets the scale (value) and precision (key).
     * 
     * @param nativeType
     *            the native data type
     * @return the scale and precision
     */
    public static TypedKeyValue<Integer, Integer> getScaleAndPrecision(
            String nativeType)
    {
        if (nativeType == null)
            return null;
        
        int start = nativeType.indexOf("(");
        int end = nativeType.indexOf(")");
        
        if (start >= 0 && end > start)
        {
            String value = nativeType.substring(start + 1, end).trim();
            
            if (Utils.isNothing(value))
                return null;
            
            String[] tokens = value.split(",", -1);
            
            if (tokens.length == 1)
                return new TypedKeyValue<Integer, Integer>(Utils.str2Int(
                        tokens[0].trim(), -1), null);
            else
                return new TypedKeyValue<Integer, Integer>(Utils.str2Int(
                        tokens[0], -1), Utils.str2Int(tokens[1].trim(), -1));
            
        }
        
        return null;
    }
    
    /**
     * Gets the select clause for metadata extraction.
     * 
     * @param driver
     *            the driver
     * @return the select clause
     */
    public static String getSelectClause(Driver driver)
    {
        String clause = driver.getMetadataSelectClause();
        
        if (clause != null)
            return clause;
        else
            return "select ";
    }
    
    /**
     * Gets the select sql.
     * 
     * <p>
     * Example:
     * <p>
     * SqlUtils.getSelectSql("select * from abc where a=1") -> select * from abc
     * where a=1
     * <p>
     * SqlUtils.getSelectSql("abc") -> select * from abc
     * 
     * @param sql
     *            the sql
     * @return the select sql
     */
    public static String getSelectSql(String sql)
    {
        if (isTableName(sql))
            return "select * from " + sql;
        else
            return sql;
    }
    
    /**
     * Gets the sql for explain plan.
     * 
     * @param sql
     *            the sql
     * @param driver
     *            the driver
     * @param sqlParser
     *            the sql parser
     * @return the sql for explain plan
     */
    public static String getSqlForExplainPlan(String sql, Driver driver,
            SqlParser sqlParser)
    {
        if (Utils.isNothing(sql))
            return sql;
        
        Map<String, Object> properties = new LinkedHashMap<String, Object>();
        
        if (sqlParser == null)
            sqlParser = (SqlParser)ObjectFactory.instance().get(
                    SqlParser.class.getName(),
                    GenericSqlParser.class.getName(), true);
        
        sqlParser.parseParams(sql, properties, null, null, driver);
        
        for (String name : properties.keySet())
        {
            properties.put(name, "null");
        }
        
        return sqlParser.setBindVariables(sql, properties);
    }
    
    /**
     * Gets the sql for metadata extraction.
     * 
     * @param objectName
     *            the object name
     * @param driver
     *            the driver
     * @return the sql for metadata extraction
     */
    public static String getSqlForMetadataExtraction(String objectName,
            Driver driver)
    {
        return isTableName(objectName) ? SqlUtils.getSelectClause(driver)
                + "* from " + objectName + SqlUtils.getWhereClause(driver)
                : objectName;
    }
    
    /**
     * Gets the type (key) and value (value.
     * 
     * @param text
     *            the text
     * @param params
     *            the parameters
     * @return the type and value
     */
    public static TypedKeyValue<Integer, Object> getTypeAndValue(String text,
            Map<String, String> params)
    {
        if (text == null || text.length() == 0)
            return new TypedKeyValue<Integer, Object>(Types.VARCHAR, null);
        
        String charSeparator = params != null ? params.get(CHAR_SEPARATOR_PROP)
                : "";
        
        if (!Utils.isNothing(charSeparator)
                && text.charAt(0) == charSeparator.charAt(0)
                && text.charAt(text.length() - 1) == charSeparator.charAt(0))
        {
            return new TypedKeyValue<Integer, Object>(Types.VARCHAR,
                    text.substring(1, text.length() - 1));
        }
        
        if (Utils.isNumber(text))
        {
            TypedKeyValue<Integer, Number> ret = getNumberTypeAndValue(text);
            
            if (ret != null)
                return new TypedKeyValue<Integer, Object>(ret.getKey(),
                        ret.getValue());
            else
                return new TypedKeyValue<Integer, Object>(Types.VARCHAR, text);
        }
        else
        {
            Date date = str2DateTime(text, params);
            
            if (date != null)
                return new TypedKeyValue<Integer, Object>(Types.TIMESTAMP, date);
            else
                date = str2Time(text, params);
            
            if (date != null)
                return new TypedKeyValue<Integer, Object>(Types.TIME, date);
            else
                date = str2Date(text, params);
            
            if (date != null)
                return new TypedKeyValue<Integer, Object>(Types.DATE, date);
        }
        
        return new TypedKeyValue<Integer, Object>(Types.VARCHAR, text);
    }
    
    /**
     * Gets the sql type {@link java.sql.Types} by type name.
     *
     * @param name the type name
     * @return the sql type
     */
    public static Integer getTypeByName(String name)
    {
        if (Utils.isNothing(name))
            return null;
        
        return TYPE_NAMES.get(name);
    }
    
    /**
     * Gets the type name using given {@link java.sql.Types}.
     * 
     * @param fieldType
     *            the field type
     * @return the java type name
     */
    public static String getTypeName(Object fieldType)
    {
        if (fieldType == null)
            return "Unknown";
        
        int fldType;
        
        if (fieldType instanceof Number)
            fldType = ((Number)fieldType).intValue();
        else
            try
            {
                fldType = Integer.valueOf(fieldType.toString());
            }
            catch (Throwable ex)
            {
                return "Unknown";
            }
        
        String value = TYPES.get(fldType);
        
        return Utils.isNothing(value) ? fieldType.toString() : value;
    }
    
    /**
     * Gets the type range.
     * 
     * <p>
     * Example:
     * <p>
     * SqlUtils.getTypeRange("number(100, 20)", 10, 30) -> (10,20)
     * 
     * @param typeName
     *            the type name
     * @param maxPrec
     *            the maximum precision
     * @param maxScale
     *            the maximum scale
     * @return the type range
     */
    public static String getTypeRange(String typeName, int maxPrec, int maxScale)
    {
        return getTypeRange(typeName, maxPrec, maxScale, false);
    }
    
    /**
     * Gets the type range.
     * 
     * <p>
     * Example:
     * <p>
     * SqlUtils.getTypeRange("number(100, 20)", 10, 30) -> (10,20)
     *
     * @param typeName the type name
     * @param maxPrec the maximum precision
     * @param maxScale the maximum scale
     * @param scaleLessOrEqualPrec if true the scale should be less or equal to precision
     * @return the type range
     */
    public static String getTypeRange(String typeName, int maxPrec,
            int maxScale, boolean scaleLessOrEqualPrec)
    {
        int start = typeName.indexOf("(");
        int end = typeName.indexOf(")");
        
        String scale = null;
        String prec = null;
        
        if (start >= 0 && end > start)
        {
            if (maxPrec < 0)
                return "";
            
            if (end - start == 1)
            {
                if (maxScale > 0)
                    return "("
                            + maxPrec
                            + ","
                            + (!scaleLessOrEqualPrec ? maxScale
                                    : (maxScale > maxPrec ? maxPrec : maxScale))
                            + ")";
                else
                    return "(" + maxPrec + ")";
            }
            
            String s = typeName.substring(start + 1, end);
            
            String[] range = s.split(",", -1);
            
            prec = range[0].trim();
            if (range.length == 2)
                scale = range[1].trim();
            
            try
            {
                int realPrec = Integer.parseInt(prec);
                
                if (realPrec >= maxPrec)
                {
                    if (maxPrec <= 0)
                        return "";
                    
                    prec = String.valueOf(maxPrec);
                }
            }
            catch (Exception ex)
            {
                if (maxPrec <= 0)
                    return "";
                
                prec = String.valueOf(maxPrec);
            }
            
            s = "(" + prec;
            
            if (scale != null
                    && !("-1".equalsIgnoreCase(scale) || "0"
                            .equalsIgnoreCase(scale)))
            {
                int realScale = Integer.parseInt(scale);
                
                if (realScale >= maxScale)
                    scale = String.valueOf(maxScale);
                
                if (scaleLessOrEqualPrec)
                {
                    int thePrec = Integer.valueOf(prec);
                    
                    int theScale = Integer.valueOf(scale);
                    
                    if (theScale > thePrec)
                        scale = prec;
                    
                }
                
                s = s + "," + scale;
            }
            
            s = s + ")";
            
            return s;
        }
        else if (maxPrec <= 0)
            return "";
        else
            return "(" + maxPrec + ")";
        
    }
    
    /**
     * Gets the unquoted text.
     *
     * @param params the params
     * @param text the text
     * @return the unquoted text
     */
    public static String getUnquotedText(Map<String, String> params, String text)
    {
        String charSeparator = params != null ? params.get(CHAR_SEPARATOR_PROP)
                : "";
        
        if (text != null && !Utils.isNothing(charSeparator)
                && text.charAt(0) == charSeparator.charAt(0)
                && text.charAt(text.length() - 1) == charSeparator.charAt(0))
        {
            return text.substring(1, text.length() - 1);
        }
        
        return text;
    }
    
    /**
     * Gets the value.
     * 
     * <p>
     * Example;
     * <p>
     * SqlUtils.getValue("123;456;789", 1, ";") -> 456
     * 
     * @param value
     *            the value
     * @param index
     *            the index
     * @param delim
     *            the delimiter
     * @return the value
     */
    public static String getValue(String value, int index, String delim)
    {
        if (Utils.isNothing(value))
            return value;
        
        String[] values = value.split(delim, -1);
        
        if (index < 0)
            return values[values.length - 1];
        else if (index >= values.length)
            return null;
        else
            return values[index];
    }
    
    /**
     * Gets the bind variable type.
     * 
     * @param bindVar
     *            the variable value
     * @return the bind variable type {@link java.sql.Types}
     */
    public static int getVarType(String bindVar)
    {
        boolean isCont = false;
        
        if (bindVar == null || "null".equalsIgnoreCase(bindVar))
            return Types.VARCHAR;
        
        try
        {
            Long.parseLong(bindVar);
            
            return Types.INTEGER;
        }
        catch (Exception ex)
        {
            isCont = true;
        }
        
        if (isCont)
            try
            {
                Integer.parseInt(bindVar);
                
                return Types.SMALLINT;
            }
            catch (Exception ex)
            {
                isCont = true;
            }
        
        if (isCont)
            try
            {
                Double.parseDouble(bindVar);
                
                return Types.DOUBLE;
            }
            catch (Exception ex)
            {
                isCont = true;
            }
        
        if (isCont)
            try
            {
                Float.parseFloat(bindVar);
                
                return Types.FLOAT;
            }
            catch (Exception ex)
            {
                isCont = true;
            }
        if (isCont)
        {
            try
            {
                Date date = DateUtil.parse(bindVar);
                
                if (date != null)
                    if (Utils.isDateOnly(date, null))
                        return Types.DATE;
                    else if (Utils.isTimeOnly(date, null))
                        return Types.TIME;
                    else
                        return Types.TIMESTAMP;
            }
            catch (ParseException ex)
            {
                isCont = true;
            }
        }
        
        return Types.VARCHAR;
    }
    
    /**
     * Gets the variable type by object type.
     * 
     * @param bindVar
     *            the bind variable value
     * @return the bind variable type {@link java.sql.Types}
     */
    public static int getVarTypeByObjType(Object bindVar)
    {
        if (bindVar == null)
            return Types.VARCHAR;
        
        if (bindVar instanceof String)
            return Types.VARCHAR;
        
        if (bindVar instanceof Number)
            return Types.NUMERIC;
        
        if (bindVar instanceof Date)
            return Types.DATE;
        
        return Types.VARCHAR;
    }
    
    /**
     * Gets the where clause for metadata extraction.
     * 
     * @param driver
     *            the driver
     * @return the where clause
     */
    public static String getWhereClause(Driver driver)
    {
        String clause = driver.getMetadataWhereClause();
        
        if (clause != null)
            return clause;
        else
            return "";
    }
    
    /**
     * Converts date to string.
     * 
     * @param fieldValue
     *            the field value
     * @param params
     *            the parameters
     * @param paramName
     *            the parameter name
     * @param defaultValue
     *            the default value
     * @return the string
     */
    private static String internalDate2Str(Object fieldValue,
            Map<String, String> params, String paramName, String defaultValue)
    {
        if (fieldValue instanceof String)
            return (String)fieldValue;
        else if (fieldValue instanceof Date)
            return Utils.date2Str((Date)fieldValue,
                    Utils.getParamFromMap(paramName, params, defaultValue));
        else if (fieldValue != null)
            return fieldValue.toString();
        else
            return null;
    }
    
    /**
     * Checks if fType is blob.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is blob
     */
    public static boolean isBlob(int fType)
    {
        return fType == Types.BLOB || fType == Types.VARBINARY
                || fType == Types.BINARY || fType == Types.LONGVARBINARY;
    }
    
    /**
     * Checks if fType is boolean.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is boolean
     */
    public static boolean isBoolean(int fType)
    {
        return fType == Types.BOOLEAN;
    }
    
    /**
     * Checks if fType is char.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is char
     */
    public static boolean isChar(int fType)
    {
        return fType == Types.VARCHAR || fType == Types.CHAR
                || fType == SqlUtils.LONGNVARCHAR || fType == Types.LONGVARCHAR
                || fType == SqlUtils.NCHAR || fType == SqlUtils.NVARCHAR;
    }
    
    /**
     * Checks if fType is clob.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is clob
     */
    public static boolean isClob(int fType)
    {
        return fType == Types.CLOB || fType == Types.LONGVARCHAR
                || fType == SqlUtils.LONGNVARCHAR || fType == SqlUtils.NCLOB
                || fType == SQLXML;
    }
    
    /**
     * Checks if types are compatible.
     * 
     * @param original
     *            the original type {@link java.sql.Types}
     * @param compareTo
     *            the compare to type {@link java.sql.Types}
     * @return true, if is compatible
     */
    public static boolean isCompatible(int original, int compareTo)
    {
        if (original == compareTo)
            return true;
        
        Integer value = COMPAT_TYPES.get(original + "|" + compareTo);
        
        return value != null && value == original;
    }
    
    /**
     * Checks if fType is date.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is date
     */
    public static boolean isDate(int fType)
    {
        return fType == Types.DATE || fType == Types.TIME
                || fType == Types.TIMESTAMP;
    }
    
    /**
     * Checks if fType is date only.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is date only
     */
    public static boolean isDateOnly(int fType)
    {
        return fType == Types.DATE;
    }
    
    /**
     * Checks if fType is date+time.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is date+time
     */
    public static boolean isDateTime(int fType)
    {
        return fType == Types.DATE || fType == Types.TIMESTAMP;
    }
    
    /**
     * Checks if fType is decimal.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is decimal
     */
    public static boolean isDecimal(int fType)
    {
        return fType == Types.FLOAT || fType == Types.REAL
                || fType == Types.DOUBLE || fType == Types.DECIMAL
                || fType == Types.NUMERIC;
    }
    
    /**
     * Checks if fType is large object.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is large object
     */
    public static boolean isLargeObject(int fType)
    {
        return isBlob(fType) || isClob(fType);
    }
    
    /**
     * Checks if fType is number.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is number
     */
    public static boolean isNumber(int fType)
    {
        return fType == Types.BIT || fType == Types.TINYINT
                || fType == Types.SMALLINT || fType == Types.INTEGER
                || fType == Types.BIGINT || fType == Types.FLOAT
                || fType == Types.REAL || fType == Types.DOUBLE
                || fType == Types.DECIMAL || fType == Types.NUMERIC;
        
    }
    
    /**
     * Checks if fType is other.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is other
     */
    public static boolean isOther(int fType)
    {
        return fType == Types.NULL || fType == Types.DISTINCT
                || fType == Types.REF || fType == Types.DATALINK;
    }
    
    /**
     * Checks if fType is struct.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is struct
     */
    public static boolean isStruct(int fType)
    {
        return fType == Types.STRUCT;
    }
    
    /**
     * Checks if given sql is a table name.
     * 
     * @param sql
     *            the sql
     * @return true, if is table name
     */
    public static boolean isTableName(String sql)
    {
        return sql != null && sql.trim().split(" ").length == 1;
    }
    
    /**
     * Checks if fType is time.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is time
     */
    public static boolean isTime(int fType)
    {
        return fType == Types.TIME;
    }
    
    /**
     * Checks if fType is timestamp.
     * 
     * @param fType
     *            the field type {@link java.sql.Types}
     * @return true, if is timestamp
     */
    public static boolean isTimestamp(int fType)
    {
        return fType == Types.TIMESTAMP;
    }
    
    /**
     * Converts name to the right case.
     * 
     * @param driver
     *            the driver
     * @param name
     *            the name
     * @return the string
     */
    public static String name2RightCase(Driver driver, String name)
    {
        if (driver.getCaseSensitive() == Driver.CASE_SENSITIVE_LOWER)
            return name.toLowerCase();
        else if (driver.getCaseSensitive() == Driver.CASE_SENSITIVE_UPPER)
            return name.toUpperCase();
        else
            return name;
    }
    
    /**
     * Splits given sql on multiple sql statements using ";" as a separator.
     * Ignores ";" in comments and parameters.
     * 
     * @param sql
     *            the sql
     * @return the sql statements
     */
    public static String[] parseSql(String sql)
    {
        if (Utils.isNothing(sql))
            return null;
        
        SqlParser sqlParser = (SqlParser)ObjectFactory.instance().get(
                SqlParser.class.getName(), GenericSqlParser.class.getName(),
                true);
        
        return sqlParser.split(sql);
    }
    
    /**
     * Populates data set from the result set.
     * 
     * @param dataSet
     *            the data set
     * @param driver
     *            the driver
     * @param rs
     *            the result set
     * @param fieldsMapping
     *            the fields mapping. Maps original fields names to the new. Can
     *            be null.
     * @param unique
     *            the "is unique" flag, if true will exclude duplicated records
     * @param checkKeyField
     *            the "check key field" flag, if true and dataSet.getKeyField()
     *            != null ignores null key fields
     * @throws Exception
     *             in case of any error
     */
    public static void populateDataSet(DataSet dataSet, Driver driver,
            ResultSet rs, ListHashMap<String, String> fieldsMapping,
            boolean unique, boolean checkKeyField)
        throws Exception
    {
        populateDataSet(dataSet, driver, rs, fieldsMapping, unique,
                checkKeyField, null, null);
    }
    
    /**
     * Populates data set from the result set.
     * 
     * @param dataSet
     *            the data set
     * @param driver
     *            the driver
     * @param rs
     *            the result set
     * @param fieldsMapping
     *            the fields mapping. Maps original fields names to the new. Can
     *            be null.
     * @param unique
     *            the "is unique" flag, if true will exclude duplicated records
     * @param checkKeyField
     *            the "check key field" flag, if true and dataSet.getKeyField()
     *            != null ignores null key fields
     * @param filterByField
     *            the field name to filter by
     * @param filterByFieldValue
     *            the field value to filter by. Works together with
     *            filterByField
     * @throws Exception
     *             in case of any error
     */
    public static void populateDataSet(DataSet dataSet, Driver driver,
            ResultSet rs, ListHashMap<String, String> fieldsMapping,
            boolean unique, boolean checkKeyField, String filterByField,
            String filterByFieldValue)
        throws Exception
    {
        populateDataSet(dataSet, driver, rs, fieldsMapping, unique,
                checkKeyField, filterByField, filterByFieldValue, -1);
    }
    
    /**
     * Populates data set from the result set.
     * 
     * @param dataSet
     *            the data set
     * @param driver
     *            the driver
     * @param rs
     *            the result set
     * @param fieldsMapping
     *            the fields mapping. Maps original fields names to the new. Can
     *            be null.
     * @param unique
     *            the "is unique" flag, if true will exclude duplicated records
     * @param checkKeyField
     *            the "check key field" flag, if true and dataSet.getKeyField()
     *            != null ignores null key fields
     * @param filterByField
     *            the field name to filter by
     * @param filterByFieldValue
     *            the field value to filter by. Works together with
     *            filterByField
     * @param maxRows
     *            the maximum number of rows allowed for the data set. maxRows =
     *            -1 means there is no limitation
     * @throws Exception
     *             in case of any error
     */
    public static void populateDataSet(DataSet dataSet, Driver driver,
            ResultSet rs, ListHashMap<String, String> fieldsMapping,
            boolean unique, boolean checkKeyField, String filterByField,
            String filterByFieldValue, int maxRows)
        throws Exception
    {
        SqlConnectorParams params = new SqlConnectorParams(rs, null, true, -1);
        
        params.setFieldsMapping(fieldsMapping);
        params.setUnique(unique);
        params.setCheckKeyField(checkKeyField);
        params.setFilterByField(filterByField);
        params.setFilterByFieldValue(filterByFieldValue);
        if (maxRows >= 0)
            params.setMaxRows(maxRows);
        
        SqlConnector sqlConnector = new SqlConnector();
        
        sqlConnector.populate(params, dataSet, driver);
    }
    
    /**
     * Reads value for the <code>field</code> from the result set
     * <code>rs</code> and adds it to the <code>params</code>. Returns params.
     * 
     * <p>
     * Example:
     * <p>
     * abc -> "'abc'"
     * <p>
     * 123 -> 'abc',123
     * <p>
     * xyz -> 'abc',123,'xyz'
     * 
     * @param rs
     *            the result set
     * @param params
     *            the existing parameters
     * @param field
     *            the field
     * @return the string
     * @throws Exception
     *             in case of any error
     */
    public static String prepareParams(ResultSet rs, String params, String field)
        throws Exception
    {
        Object param = null;
        
        if (Utils.isNothing(field))
            param = rs.getObject(1);
        else
            param = rs.getObject(field);
        
        return prepareParams(params, param);
    }
    
    /**
     * Prepare parameters.
     * 
     * <p>
     * Example:
     * <p>
     * abc -> "'abc'"
     * <p>
     * 123 -> 'abc',123
     * <p>
     * xyz -> 'abc',123,'xyz'
     * 
     * @param params
     *            the existing parameters
     * @param param
     *            the parameter to add
     * @return the string
     */
    public static String prepareParams(String params, Object param)
    {
        if (param != null)
        {
            if (param instanceof Number)
                param = param.toString();
            else
                param = "'" + param.toString() + "'";
            
            if (Utils.isNothing(params))
                params = (String)param;
            else
                params = params + "," + param;
        }
        
        return params;
    }
    
    /**
     * Prepares sql statement. Replaces field names on "?" for bind variables
     * and on real field values for everything else.
     * 
     * @param sql
     *            The sql statement
     * @param dataSet
     *            the data set
     * @param index
     *            the index
     * @param driver
     *            the driver
     * @param bindVariables
     *            The bind variables
     * @param using
     *            The using for bind variables
     * @return new sql
     */
    private static String prepareSql(String sql, DataSet dataSet, int index,
            Driver driver, Map<String, Object> bindVariables, String using)
    {
        String fieldName;
        DataSetRecord currentRow = index >= 0 ? dataSet.getRecord(index) : null;
        
        if (currentRow == null)
            return sql;
        
        String[] usingVars = null;
        
        if (!Utils.isNothing(using))
            usingVars = using.split(",");
        
        int fieldCount = dataSet.getFieldCount();
        
        for (int i = 0; i < fieldCount; i++)
        {
            FieldDef fieldDef = dataSet.getFieldDef(i);
            
            fieldName = fieldDef.getName();
            int fieldType = fieldDef.getSqlDataType();
            Object fieldValue = currentRow.get(i);
            
            if (bindVariables != null && usingVars != null
                    && usingVars.length > 0)
            {
                if (addBindVar(usingVars, fieldName, fieldValue, bindVariables))
                    sql = Utils.findAndReplaceRegexp(sql,
                            "{" + fieldName + "}", "?", true);
                else
                    sql = Utils.findAndReplace(sql, "{" + fieldName + "}",
                            driver.convertValueForStorage(fieldValue,
                                    fieldType, false), true);
            }
            else
                sql = Utils.findAndReplace(sql, "{" + fieldName + "}", driver
                        .convertValueForStorage(fieldValue, fieldType, false),
                        true);
        }
        
        return sql;
    }
    
    /**
     * Checks if value of the field of the type <code>fType</code> requires
     * translation to display.
     * 
     * @param fType
     *            the type pf the field {@link java.sql.Types}
     * @return true, if successfuls
     */
    public static boolean requiresTranslation(int fType)
    {
        return isDate(fType) || isDecimal(fType);
    }
    
    /**
     * Returns entire result set as a text. Uses "|" as a delimiter for fields.
     * 
     * @param driver
     *            the driver
     * @param rs
     *            the result set
     * @param fields
     *            the fields
     * @return the string
     * @throws Exception
     *             in case of any error
     */
    public static String resultSet2Text(Driver driver, ResultSet rs,
            String fields)
        throws Exception
    {
        DataSet dataSet = new DataSet();
        dataSet.setName("convert");
        
        SqlConnectorParams params = new SqlConnectorParams(rs, null, true, -1);
        
        SqlConnector sqlConnector = new SqlConnector();
        
        sqlConnector.populate(params, dataSet, driver);
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams textParams = new TextConnectorParams(
                new EtlConfig(), false, -1, "|", true);
        textParams.setFields(fields);
        textParams.setFirstRowData(false);
        
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        
        textParams.setOutputStream(output);
        
        textConnector.persist(textParams, dataSet, driver);
        
        return new String(output.toByteArray());
    }
    
    /**
     * Sets the bind variable.
     * 
     * @param statement
     *            the statement
     * @param bindVar
     *            the bind variable
     * @param index
     *            the index
     * @throws Exception
     *             in case of any error
     */
    public static void setBindVar(PreparedStatement statement, Object bindVar,
            int index)
        throws Exception
    {
        int type = getVarTypeByObjType(bindVar);
        
        setBindVar(statement, bindVar, type, index);
    }
    
    /**
     * Sets the bind variable.
     * 
     * @param statement
     *            the statement
     * @param bindVar
     *            the bind variable
     * @param type
     *            the type {@link java.sql.Types}
     * @param index
     *            the index
     * @throws Exception
     *             in case of any error
     */
    public static void setBindVar(PreparedStatement statement, Object bindVar,
            int type, int index)
        throws Exception
    {
        if (bindVar == null)
        {
            statement.setNull(index, type);
            return;
        }
        
        if (isNumber(type))
        {
            statement.setBigDecimal(index, new BigDecimal(bindVar.toString()));
            return;
        }
        
        switch (type)
        {
            case Types.DATE:
                statement.setDate(index,
                        new java.sql.Date(((Date)bindVar).getTime()));
                return;
            case Types.TIME:
                statement.setTime(index,
                        new java.sql.Time(((Date)bindVar).getTime()));
                return;
            case Types.TIMESTAMP:
                statement.setTimestamp(index, new java.sql.Timestamp(
                        ((Date)bindVar).getTime()));
                return;
            default:
                statement.setString(index, (String)bindVar);
        }
    }
    
    /**
     * Sets the bind variable.
     * 
     * @param statement
     *            the statement
     * @param bindVar
     *            the bind variable
     * @param index
     *            the index
     * @throws Exception
     *             in case of any error
     */
    public static void setBindVar(PreparedStatement statement, String bindVar,
            int index)
        throws Exception
    {
        int type = getVarType(bindVar);
        
        setBindVar(statement, bindVar, type, index);
    }
    
    /**
     * Sets the bind variable.
     * 
     * @param statement
     *            the statement
     * @param bindVar
     *            the bind variable
     * @param type
     *            the type
     * @param index
     *            the index
     * @throws Exception
     *             in case of any error
     */
    public static void setBindVar(PreparedStatement statement, String bindVar,
            int type, int index)
        throws Exception
    {
        if (bindVar == null)
        {
            statement.setNull(index, type);
            return;
        }
        
        switch (type)
        {
            case Types.BIGINT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.BIT:
                statement.setLong(index, Long.parseLong(bindVar));
                return;
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.REAL:
                statement.setDouble(index, Double.parseDouble(bindVar));
                return;
            case Types.FLOAT:
                statement.setFloat(index, Float.parseFloat(bindVar));
                return;
            case Types.DATE:
                statement.setDate(index,
                        new java.sql.Date(DateUtil.parse(bindVar).getTime()));
                return;
            case Types.TIME:
                statement.setTime(index,
                        new java.sql.Time(DateUtil.parse(bindVar).getTime()));
                
                return;
            case Types.TIMESTAMP:
                statement.setTimestamp(index, new java.sql.Timestamp(DateUtil
                        .parse(bindVar).getTime()));
                return;
            default:
                statement.setString(index, bindVar);
        }
    }
    
    /**
     * Sets the blob.
     * 
     * @param pstmt
     *            the prepared statement
     * @param value
     *            the value
     * @param pos
     *            the position of the blob field
     * @throws Exception
     *             in case of any error
     */
    public static void setBlob(PreparedStatement pstmt, Object value, int pos)
        throws Exception
    {
        if (value == null)
            pstmt.setNull(pos, Types.BLOB);
        else
        {
            InputStream is = new ByteArrayInputStream((byte[])value);
            pstmt.setBinaryStream(pos, is, is.available());
        }
    }
    
    /**
     * Sets the clob.
     * 
     * @param pstmt
     *            the preapred statement
     * @param value
     *            the value
     * @param pos
     *            the position of the clob field
     * @throws Exception
     *             in case of any error
     */
    public static void setClob(PreparedStatement pstmt, Object value, int pos)
        throws Exception
    {
        if (value == null)
            pstmt.setNull(pos, Types.CLOB);
        else
            pstmt.setString(pos, value.toString());
    }
    
    /**
     * Converts field value.
     * 
     * @param fieldType
     *            the field type {@link java.sql.Types}
     * @param value
     *            the value
     * @param params
     *            the parameters
     * @return the object
     */
    public static Object storageValue2Value(int fieldType, Object value,
            Map<String, String> params)
    {
        if (value == null)
            return null;
        
        switch (fieldType)
        {
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return Utils.str2Integer(value.toString(), null);
            case Types.BIGINT:
                return Utils.str2Long(value.toString(), null);
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.DECIMAL:
            case Types.NUMERIC:
                return Utils.str2Number(value.toString(), null);
            case Types.CHAR:
            case Types.LONGVARCHAR:
            case SqlUtils.NCHAR:
            case SqlUtils.NVARCHAR:
            case SqlUtils.LONGNVARCHAR:
            case Types.VARCHAR:
                return value.toString();
            case Types.DATE:
                return str2Date(value.toString(), params);
            case Types.TIME:
                return str2Time(value.toString(), params);
            case Types.TIMESTAMP:
                return str2DateTime(value.toString(), params);
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
            case Types.CLOB:
            case SqlUtils.NCLOB:
            case Types.REF:
            case Types.DATALINK:
                return value;
            case Types.BOOLEAN:
                return Utils.str2Boolean(value.toString(), null);
            case SqlUtils.ROWID:
                return value;
            case SqlUtils.SQLXML:
                return value;
        }
        
        return value;
    }
    
    /**
     * Converts text to date using value for the DATE_FORMAT_PROP property as a
     * format string. If not found in the <code>params</code> uses default
     * DataSet.DATA_SET_DATE_TIME_FORMAT.
     * 
     * @param text
     *            the text
     * @param params
     *            the parameters
     * @return the date
     */
    public static Date str2Date(String text, Map<String, String> params)
    {
        if (text == null)
            return null;
        
        return Utils.str2Date(text, null, Utils.getParamFromMap(
                DATE_FORMAT_PROP, params, DataSet.DATA_SET_DATE_TIME_FORMAT));
    }
    
    /**
     * Converts text to date+time using value for the DATE_TIME_FORMAT_PROP
     * property as a format string. If not found in the <code>params</code> uses
     * default DataSet.DATA_SET_DATE_TIME_FORMAT.
     * 
     * @param text
     *            the text
     * @param params
     *            the parameters
     * @return the date
     */
    public static Date str2DateTime(String text, Map<String, String> params)
    {
        if (text == null)
            return null;
        
        return Utils.str2Date(text, null, Utils.getParamFromMap(
                DATE_TIME_FORMAT_PROP, params,
                DataSet.DATA_SET_DATE_TIME_FORMAT));
    }
    
    /**
     * Converts text to time using value for the TIME_FORMAT_PROP property as a
     * format string. If not found in the <code>params</code> uses default
     * DataSet.DATA_SET_TIME_FORMAT.
     * 
     * @param text
     *            the text
     * @param params
     *            the parameters
     * @return the date
     */
    public static Date str2Time(String text, Map<String, String> params)
    {
        if (text == null)
            return null;
        
        return Utils.str2Date(text, null, Utils.getParamFromMap(
                TIME_FORMAT_PROP, params, DataSet.DATA_SET_TIME_FORMAT));
        
    }
    
    /**
     * Converts time to the string using value for the TIME_FORMAT_PROP property
     * as a format string. If not found in the <code>params</code> uses default
     * DataSet.DATA_SET_TIME_FORMAT.
     * 
     * @param fieldValue
     *            the field value
     * @param params
     *            the params
     * @return the string
     */
    public static String time2Str(Object fieldValue, Map<String, String> params)
    {
        return internalDate2Str(fieldValue, params, TIME_FORMAT_PROP,
                DataSet.DATA_SET_TIME_FORMAT);
    }
    
    /**
     * Convrerts field value to the display value. Display value used in the
     * grid.
     * 
     * @param fieldType
     *            the field type {@link java.sql.Types}
     * @param value
     *            the value
     * @param params
     *            the parameters
     * @return the object
     */
    public static Object value2DisplayValue(int fieldType, Object value,
            Map<String, String> params)
    {
        if (value == null)
            return null;
        else
        {
            switch (fieldType)
            {
                case Types.DATE:
                    return date2Str(value, params);
                case Types.TIME:
                    return time2Str(value, params);
                case Types.TIMESTAMP:
                    return dateTime2Str(value, params);
            }
            
            if (isDecimal(fieldType))
                return decimal2Str(value, params);
            else if (isClob(fieldType))
                return value;
            else if (isBlob(fieldType))
                return "Blob";
            else if (value instanceof byte[])
            {
                return new String((byte[])value);
            }
            
            return value.toString();
        }
    }
    
    /**
     * Converts value to the storage value. Storage value used in sql DML
     * statements.
     * 
     * @param fieldType
     *            the field type {@link java.sql.Types}
     * @param value
     *            the value
     * @param params
     *            the parameters
     * @return the object
     */
    public static Object value2StorageValue(int fieldType, Object value,
            Map<String, String> params)
    {
        if (value == null)
            return null;
        else
        {
            switch (fieldType)
            {
                case Types.DATE:
                    return date2Str(value, params);
                case Types.TIME:
                    return time2Str(value, params);
                case Types.TIMESTAMP:
                    return dateTime2Str(value, params);
            }
            
            if (isLargeObject(fieldType))
                return value;
            
            return value.toString();
        }
    }
}
