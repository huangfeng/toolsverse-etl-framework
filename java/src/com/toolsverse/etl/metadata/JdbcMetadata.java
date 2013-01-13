/*
 * JdbcMetadata.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.metadata;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetData;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.connection.ConnectionFactory;
import com.toolsverse.etl.sql.connection.GenericConnectionFactory;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.KeyValue;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * The implementation of the Metadata interface for the jdbc complaint
 * databases.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class JdbcMetadata extends BaseMetadata
{
    
    /**
     * The Class DbMetadata.
     */
    public class DbMetadata
    {
        
        /** The connection. */
        private Connection _connection = null;
        
        /** The database meta data. */
        private DatabaseMetaData _databaseMetaData = null;
        
        /** The connection factory. */
        private ConnectionFactory _connectionFactory = null;
        
        /**
         * Instantiates a new DbMetadata.
         * 
         * @param connectionFactory
         *            the connection factory
         * @throws Exception
         *             in case of any error
         */
        public DbMetadata(ConnectionFactory connectionFactory) throws Exception
        {
            if (connectionFactory == null)
                _connectionFactory = (ConnectionFactory)ObjectFactory
                        .instance().get(ConnectionFactory.class.getName(),
                                GenericConnectionFactory.class.getName(), true);
            else
                _connectionFactory = connectionFactory;
            
            _connection = _connectionFactory
                    .getConnection(getConnectionParamsProvider()
                            .getConnectionParams());
            
            _databaseMetaData = _connection.getMetaData();
        }
        
        /**
         * Releases connection.
         */
        public void free()
        {
            if (_connectionFactory != null)
                _connectionFactory.releaseConnection(_connection);
        }
        
        /**
         * Gets the connection.
         * 
         * @return the connection
         */
        public Connection getConnection()
        {
            return _connection;
        }
        
        /**
         * Gets the database meta data.
         * 
         * @return the database meta data
         */
        public DatabaseMetaData getDatabaseMetaData()
        {
            return _databaseMetaData;
        }
        
    }
    
    /** The NAME. */
    private static final String NAME = "Jdbc";
    
    /** The type methods. */
    protected static Map<String, String> TYPE_METHODS = new HashMap<String, String>();
    
    /** The metadata methods. */
    protected static Map<String, String> METADATA_METHODS = new HashMap<String, String>();
    
    /** The types by parent. */
    protected static Map<String, String> TYPES_BY_PARENT = new HashMap<String, String>();
    
    /** The types which support asText method. */
    protected static Map<String, String> TYPE_SUPPORT_AS_TEXT = new HashMap<String, String>();
    static
    {
        TYPE_METHODS.put(TYPE_SCHEMA, "getDbObjectTypes");
        TYPE_METHODS.put(TYPE_CATALOG, "getDbObjectTypes");
        
        TYPE_METHODS.put(TYPE_DBINFO, "getDbInfoObjects");
        
        TYPE_METHODS.put(TYPE_TABLE, "getTableMetadataTypes");
        TYPE_METHODS.put(TYPE_VIEW, "getViewMetadataTypes");
        
        TYPE_METHODS.put(TYPE_PROC, "getProcMetadataTypes");
        TYPE_METHODS.put(TYPE_FUNCT, "getFunctMetadataTypes");
        
        TYPE_METHODS.put(TYPE_INDEX, "getIndexMetadataTypes");
        TYPE_METHODS.put(TYPE_PK, "getPKMetadataTypes");
        TYPE_METHODS.put(TYPE_FK, "getFKMetadataTypes");
        TYPE_METHODS.put(TYPE_EK, "getEKMetadataTypes");
    }
    
    static
    {
        METADATA_METHODS.put(TYPE_TYPES, "getTypes");
        METADATA_METHODS.put(TYPE_DBPROPS, "getDbProps");
        METADATA_METHODS.put(TYPE_DBFUNCTIONS, "getDbFunctions");
        METADATA_METHODS.put(TYPE_FUNCTS, "getFuncts");
        METADATA_METHODS.put(TYPE_PROCS, "getProcs");
        METADATA_METHODS.put(TYPE_PROCS_FUNCS, "getProcs");
        METADATA_METHODS.put(TYPE_TABLES, "getTablesByType");
        METADATA_METHODS.put(TYPE_VIEWS, "getTablesByType");
        METADATA_METHODS.put(TYPE_SYSTEM_TABLES, "getTablesByType");
        METADATA_METHODS.put(TYPE_GLOBAL_TEMPORARIES, "getTablesByType");
        METADATA_METHODS.put(TYPE_LOCAL_TEMPORARIES, "getTablesByType");
        METADATA_METHODS.put(TYPE_ALIASES, "getTablesByType");
        METADATA_METHODS.put(TYPE_SYNONYMS, "getTablesByType");
        METADATA_METHODS.put(TYPE_PARAMETERS, "getParameters");
        METADATA_METHODS.put(TYPE_FUNCT_PARAMETERS, "getFunctParameters");
        METADATA_METHODS.put(TYPE_PROC_PARAMETERS, "getProcParameters");
        
        METADATA_METHODS.put(TYPE_COLUMNS, "getTableColumns");
        METADATA_METHODS.put(TYPE_INDEXES, "getTableIndexes");
        METADATA_METHODS.put(TYPE_PKS, "getTablePKs");
        METADATA_METHODS.put(TYPE_FKS, "getTableFKs");
        METADATA_METHODS.put(TYPE_EKS, "getTableEKs");
        
        METADATA_METHODS.put(TYPE_INDEX_COLUMNS, "getIndexColumns");
        METADATA_METHODS.put(TYPE_PK_COLUMNS, "getPKColumns");
        METADATA_METHODS.put(TYPE_FK_COLUMNS, "getFKColumns");
        METADATA_METHODS.put(TYPE_EK_COLUMNS, "getEKColumns");
    }
    static
    {
        TYPES_BY_PARENT.put(TYPE_FUNCTS, TYPE_FUNCT);
        TYPES_BY_PARENT.put(TYPE_PROCS, TYPE_PROC);
        TYPES_BY_PARENT.put(TYPE_PROCS_FUNCS, TYPE_PROC);
        TYPES_BY_PARENT.put(TYPE_FUNCT_PARAMETERS, TYPE_PARAMETER);
        TYPES_BY_PARENT.put(TYPE_PROC_PARAMETERS, TYPE_PARAMETER);
        TYPES_BY_PARENT.put(TYPE_PARAMETERS, TYPE_PARAMETER);
        TYPES_BY_PARENT.put(TYPE_TYPES, TYPE_TYPE);
        TYPES_BY_PARENT.put(TYPE_TABLES, TYPE_TABLE);
        TYPES_BY_PARENT.put(TYPE_VIEWS, TYPE_VIEW);
        TYPES_BY_PARENT.put(TYPE_SYSTEM_TABLES, TYPE_TABLE);
        TYPES_BY_PARENT.put(TYPE_SYSTEM_VIEWS, TYPE_VIEW);
        TYPES_BY_PARENT.put(TYPE_GLOBAL_TEMPORARIES, TYPE_TABLE);
        TYPES_BY_PARENT.put(TYPE_LOCAL_TEMPORARIES, TYPE_TABLE);
        TYPES_BY_PARENT.put(TYPE_TEMPORARY_TABLES, TYPE_TABLE);
        TYPES_BY_PARENT.put(TYPE_TEMPORARY_VIEWS, TYPE_VIEW);
        TYPES_BY_PARENT.put(TYPE_ALIASES, TYPE_ALIAS);
        TYPES_BY_PARENT.put(TYPE_SYNONYMS, TYPE_SYNONYM);
        
        TYPES_BY_PARENT.put(TYPE_COLUMNS, TYPE_COLUMN);
        TYPES_BY_PARENT.put(TYPE_INDEXES, TYPE_INDEX);
        TYPES_BY_PARENT.put(TYPE_PKS, TYPE_PK);
        TYPES_BY_PARENT.put(TYPE_FKS, TYPE_FK);
        TYPES_BY_PARENT.put(TYPE_EKS, TYPE_EK);
        
        TYPES_BY_PARENT.put(TYPE_INDEX_COLUMNS, TYPE_INDEX_COLUMN);
        TYPES_BY_PARENT.put(TYPE_PK_COLUMNS, TYPE_PK_COLUMN);
        TYPES_BY_PARENT.put(TYPE_FK_COLUMNS, TYPE_FK_COLUMN);
        TYPES_BY_PARENT.put(TYPE_EK_COLUMNS, TYPE_EK_COLUMN);
    }
    
    static
    {
        TYPE_SUPPORT_AS_TEXT.put(TYPE_TABLE, "getTableAsText");
    }
    
    /**
     * Gets the function parameter type.
     * 
     * @param type
     *            the type
     * @return the function parameter type
     */
    public static String getFuncParamType(Object type)
    {
        if (type == null)
            return "Unknown";
        
        if (!(type instanceof Number))
            return type.toString();
        
        final int typeInt = ((Number)type).intValue();
        
        switch (typeInt)
        {
            case 0:
                return "Unknown";
            case 1:
                return "IN";
            case 2:
                return "INOUT";
            case 3:
                return "ResultSet Column";
            case 4:
                return "OUT";
            case 5:
                return "Return Value";
            default:
                return type.toString();
        }
    }
    
    /**
     * Gets the function return type.
     * 
     * @param type
     *            the type
     * @return the function return type
     */
    public static String getFuncRetType(Object type)
    {
        if (type == null)
            return "Unknown";
        
        if (!(type instanceof Number))
            return type.toString();
        
        final int typeInt = ((Number)type).intValue();
        
        switch (typeInt)
        {
            case 0:
                return "Unknown";
            case 1:
                return "No Table";
            case 2:
                return "Table";
            default:
                return type.toString();
        }
    }
    
    /**
     * Gets the procedure return type.
     * 
     * @param type
     *            the type
     * @return the procedure return type
     */
    public static String getProcsRetType(Object type)
    {
        if (type == null)
            return "Unknown";
        
        if (!(type instanceof Number))
            return type.toString();
        
        final int typeInt = ((Number)type).intValue();
        
        switch (typeInt)
        {
            case 0:
                return "Unknown";
            case 1:
                return "No Result";
            case 2:
                return "Returns Result";
            default:
                return type.toString();
        }
    }
    
    /**
     * Adds the functions.
     * 
     * @param dataSet
     *            the data set
     * @param param
     *            the parameter
     * @param name
     *            the name
     */
    private void addFunctions(DataSet dataSet, String param, String name)
    {
        String[] values = Utils.makeString(param).split(",", -1);
        
        for (String value : values)
        {
            if (Utils.isNothing(value))
                continue;
            
            DataSetRecord record = new DataSetRecord();
            
            record.add(name);
            record.add(value);
            
            dataSet.addRecord(record);
        }
        
    }
    
    /**
     * Adds the property.
     * 
     * @param dataSet
     *            the data set
     * @param name
     *            the name
     * @param metaData
     *            the meta data
     * @param func
     *            the function
     * @param convert
     *            the convert
     */
    private void addProp(DataSet dataSet, String name,
            DatabaseMetaData metaData, String func, String convert)
    {
        String value = getPropValue(metaData, func, convert);
        
        if (value != null)
            addProp(dataSet, name, value);
    }
    
    /**
     * Adds the property.
     * 
     * @param dataSet
     *            the data set
     * @param name
     *            the name
     * @param value
     *            the value
     */
    private void addProp(DataSet dataSet, String name, String value)
    {
        DataSetRecord record = new DataSetRecord();
        
        record.add(name);
        record.add(value);
        
        dataSet.addRecord(record);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.BaseMetadata#asText(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public String asText(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        String methodName = asTextMethod(type);
        
        if (Utils.isNothing(methodName))
            return null;
        
        Method invokeMethod = null;
        
        @SuppressWarnings("unchecked")
        Class<String>[] parametertypes = new Class[] {String.class,
                String.class, String.class, String.class};
        
        invokeMethod = getClass().getMethod(methodName, parametertypes);
        
        try
        {
            return (String)invokeMethod.invoke(this, new Object[] {catalog,
                    schema, pattern, type});
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this, Resource.ERROR_GENERAL.getValue(),
                    ex);
            
            return null;
        }
    }
    
    /**
     * asText method name for the type.
     * 
     * @param type
     *            the type
     * @return the string
     */
    public String asTextMethod(String type)
    {
        return TYPE_SUPPORT_AS_TEXT.get(type);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#discoverDatabaseTypes()
     */
    public Map<Integer, List<FieldDef>> discoverDatabaseTypes()
        throws Exception
    {
        DbMetadata dbMetadata = null;
        ResultSet rs = null;
        
        Map<Integer, List<FieldDef>> fields = new HashMap<Integer, List<FieldDef>>();
        
        try
        {
            dbMetadata = getDbMetadata();
            
            rs = dbMetadata.getDatabaseMetaData().getTypeInfo();
            
            FieldDef blob = null;
            FieldDef clob = null;
            
            while (rs.next())
            {
                FieldDef fieldDef = new FieldDef();
                
                int dataType = rs.getInt("DATA_TYPE");
                String typeName = rs.getString("TYPE_NAME");
                int precision = rs.getInt("PRECISION");
                int scale = rs.getInt("MAXIMUM_SCALE");
                boolean nullable = rs.getInt("NULLABLE") != 0;
                boolean hasParams = SqlUtils.fieldHasParams(dataType)
                        || !Utils.isNothing(rs.getString("CREATE_PARAMS"));
                
                fieldDef.setName(typeName);
                fieldDef.setSqlDataType(dataType);
                fieldDef.setNativeDataType(typeName);
                fieldDef.setPrecision(precision);
                fieldDef.setScale(scale);
                fieldDef.setNullable(nullable);
                fieldDef.setHasParams(hasParams);
                
                if (Types.BLOB == dataType)
                    blob = fieldDef;
                
                if (Types.CLOB == dataType)
                    clob = fieldDef;
                
                List<FieldDef> fieldDefs = fields.get(dataType);
                
                if (fieldDefs == null)
                {
                    fieldDefs = new ArrayList<FieldDef>();
                    
                    fields.put(dataType, fieldDefs);
                }
                
                fieldDefs.add(fieldDef);
            }
            
            for (List<FieldDef> fieldDefs : fields.values())
            {
                if (fieldDefs != null)
                    for (FieldDef fieldDef : fieldDefs)
                    {
                        
                        if (blob != null && fieldDef.isBlob()
                                && Types.BLOB != fieldDef.getSqlDataType())
                        {
                            fieldDef.setBestMatch(blob);
                        }
                        else if (clob != null && fieldDef.isClob()
                                && Types.CLOB != fieldDef.getSqlDataType())
                        {
                            fieldDef.setBestMatch(clob);
                        }
                    }
            }
        }
        finally
        {
            SqlUtils.cleanUpSQLData(null, rs, this);
            
            if (dbMetadata != null)
                dbMetadata.free();
        }
        
        return fields;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#free()
     */
    public void free()
    {
    }
    
    /**
     * Gets the catalogs.
     * 
     * @param hasSchemas
     *            the "has schemas" flag
     * @return the catalogs
     * @throws Exception
     *             in case of any error
     */
    public List<Object> getCatalogs(boolean hasSchemas)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        List<Object> list = new ArrayList<Object>();
        ResultSet rs = null;
        
        try
        {
            dbMetadata = getDbMetadata();
            
            rs = dbMetadata.getDatabaseMetaData().getCatalogs();
            
            String currentCatalog = null;
            List<Object> currentSchemas = null;
            
            try
            {
                while (rs.next())
                {
                    String catalog = rs.getString("TABLE_CAT");
                    List<Object> schemas = null;
                    if (hasSchemas)
                        schemas = getSchemas(catalog);
                    
                    if (!Utils.isNothing(getCurrentDatabase())
                            && isDatabaseCurrent(catalog))
                    {
                        currentCatalog = catalog;
                        currentSchemas = schemas;
                    }
                    else
                        list.add(new KeyValue(catalog, schemas));
                }
            }
            catch (Exception ex)
            {
                Logger.log(Logger.SEVERE, this,
                        EtlResource.ERROR_READING_CATALOGS_MSG.getValue());
                
            }
            
            if (currentCatalog != null)
                list.add(0, new KeyValue(currentCatalog, currentSchemas));
            
        }
        finally
        {
            if (rs != null)
                rs.close();
            
            if (dbMetadata != null)
                dbMetadata.free();
        }
        
        return list;
    }
    
    /**
     * Gets the catalog schema and pattern from the value. Can have database specific implementations
     *
     * @param value an array where catalog is a first element, schema is a second and pattern is a third
     * @return the catalog schema and pattern
     */
    public String[] getCatalogSchemaAndPattern(String value)
    {
        String[] ret = new String[] {null, null, null};
        
        String[] tokens = value.split(Utils.str2Regexp("."));
        
        String catalog = null;
        String schema = null;
        String pattern = null;
        
        if (tokens.length == 3)
        {
            catalog = tokens[0];
            schema = tokens[1];
            pattern = tokens[1] + METADATA_DELIMITER + tokens[2];
        }
        else if (tokens.length == 2)
        {
            pattern = value.replaceAll(Utils.str2Regexp("."),
                    METADATA_DELIMITER);
            schema = tokens[0];
        }
        else
        {
            pattern = value;
        }
        
        ret[0] = catalog;
        ret[1] = schema;
        ret[2] = pattern;
        
        return ret;
    }
    
    /**
     * Gets the database functions.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the database functions
     * @throws Exception
     *             in case of any error
     */
    public DataSet getDbFunctions(String catalog, String schema,
            String pattern, String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        try
        {
            dbMetadata = getDbMetadata();
            
            FieldDef typeField = new FieldDef();
            typeField.setName("Type");
            typeField.setSqlDataType(Types.VARCHAR);
            
            FieldDef nameField = new FieldDef();
            nameField.setName("Name");
            nameField.setSqlDataType(Types.VARCHAR);
            
            dataSet = new DataSet();
            dataSet.setName("dbfunctions");
            dataSet.addField(typeField);
            dataSet.addField(nameField);
            
            addFunctions(dataSet, dbMetadata.getDatabaseMetaData()
                    .getSQLKeywords(), "Keyword");
            
            addFunctions(dataSet, dbMetadata.getDatabaseMetaData()
                    .getNumericFunctions(), "Numeric Function");
            
            addFunctions(dataSet, dbMetadata.getDatabaseMetaData()
                    .getStringFunctions(), "String Function");
            
            addFunctions(dataSet, dbMetadata.getDatabaseMetaData()
                    .getSystemFunctions(), "System Function");
            
            addFunctions(dataSet, dbMetadata.getDatabaseMetaData()
                    .getTimeDateFunctions(), "Time and Date Function");
            
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
        
        return dataSet;
    }
    
    /**
     * Gets the database info objects.
     * 
     * @return the database info objects
     * @throws Exception
     *             in case of any error
     */
    public List<String> getDbInfoObjects()
        throws Exception
    {
        List<String> objects = new ArrayList<String>();
        
        objects.add(TYPE_DBPROPS);
        objects.add(TYPE_DBFUNCTIONS);
        
        return objects;
    }
    
    /**
     * Gets the database metadata.
     * 
     * @return the database metadata
     * @throws Exception
     *             in case of any error
     */
    public DbMetadata getDbMetadata()
        throws Exception
    {
        return new DbMetadata(getConnectionFactory());
    }
    
    /**
     * Gets the database object types.
     * 
     * @return the database object types
     * @throws Exception
     *             in case of any error
     */
    public List<String> getDbObjectTypes()
        throws Exception
    {
        List<String> types = getTableTypes();
        
        types.add(TYPE_FUNCTS);
        types.add(TYPE_PROCS);
        
        return types;
    }
    
    /**
     * Gets the database properties.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the database properties
     * @throws Exception
     *             in case of any error
     */
    public DataSet getDbProps(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        try
        {
            dbMetadata = getDbMetadata();
            
            FieldDef typeField = new FieldDef();
            typeField.setName("Name");
            typeField.setSqlDataType(Types.VARCHAR);
            
            FieldDef valueField = new FieldDef();
            valueField.setName("Value");
            valueField.setSqlDataType(Types.VARCHAR);
            
            dataSet = new DataSet();
            dataSet.setName("dbprops");
            dataSet.addField(typeField);
            dataSet.addField(valueField);
            
            addProp(dataSet, "Database Name", dbMetadata.getDatabaseMetaData(),
                    "getDatabaseProductName", null);
            
            addProp(dataSet, "Database Version",
                    dbMetadata.getDatabaseMetaData(),
                    "getDatabaseProductVersion", null);
            
            addProp(dataSet, "Driver Name", dbMetadata.getDatabaseMetaData(),
                    "getDriverName", null);
            
            addProp(dataSet,
                    "Driver Version",
                    Utils.makeString(getPropValue(
                            dbMetadata.getDatabaseMetaData(),
                            "getDriverMajorVersion", null))
                            + "."
                            + Utils.makeString(getPropValue(
                                    dbMetadata.getDatabaseMetaData(),
                                    "getDriverMinorVersion", null)));
            
            addProp(dataSet, "Url", dbMetadata.getDatabaseMetaData(), "getURL",
                    null);
            
            addProp(dataSet, "User Name", dbMetadata.getDatabaseMetaData(),
                    "getUserName", null);
            
            addProp(dataSet, "Read Only", dbMetadata.getDatabaseMetaData(),
                    "isReadOnly", null);
            
            addProp(dataSet, "Uses Local Files",
                    dbMetadata.getDatabaseMetaData(), "usesLocalFiles", null);
            
            addProp(dataSet, "Uses Local Files per Table",
                    dbMetadata.getDatabaseMetaData(), "usesLocalFilePerTable",
                    null);
            
            addProp(dataSet, "Vendor's Term for Catalog",
                    dbMetadata.getDatabaseMetaData(), "getCatalogTerm", null);
            
            addProp(dataSet, "Vendor's Term for Schema",
                    dbMetadata.getDatabaseMetaData(), "getSchemaTerm", null);
            
            addProp(dataSet, "Vendor's Term for Procedure",
                    dbMetadata.getDatabaseMetaData(), "getProcedureTerm", null);
            
            addProp(dataSet, "Supports Mixed-Case Identifiers",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsMixedCaseIdentifiers", null);
            
            addProp(dataSet, "Supports Alter table with Add",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsAlterTableWithAddColumn", null);
            
            addProp(dataSet, "Supports Alter table with Drop",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsAlterTableWithDropColumn", null);
            
            addProp(dataSet, "Supports Column Aliasing",
                    dbMetadata.getDatabaseMetaData(), "supportsColumnAliasing",
                    null);
            
            addProp(dataSet, "Supports Convert Function",
                    dbMetadata.getDatabaseMetaData(), "supportsConvert", null);
            
            addProp(dataSet, "Supports Expressions in Order By",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsExpressionsInOrderBy", null);
            
            addProp(dataSet, "Supports Order By Column Not in Select",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsOrderByUnrelated", null);
            
            addProp(dataSet, "Supports Group By",
                    dbMetadata.getDatabaseMetaData(), "supportsGroupBy", null);
            
            addProp(dataSet, "Supports Group By Column Not in Select",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsGroupByUnrelated", null);
            
            addProp(dataSet, "Supports Like Escape Clause",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsLikeEscapeClause", null);
            
            addProp(dataSet, "Supports Multiple Result Sets",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsMultipleResultSets", null);
            
            addProp(dataSet, "Supports Multiple Transactions",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsMultipleTransactions", null);
            
            addProp(dataSet, "Supports Non Nullable Columns",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsNonNullableColumns", null);
            
            addProp(dataSet, "Supports Minimum ODBC Grammar",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsMinimumSQLGrammar", null);
            
            addProp(dataSet, "Supports ODBC Core Grammar",
                    dbMetadata.getDatabaseMetaData(), "supportsCoreSQLGrammar",
                    null);
            
            addProp(dataSet, "Supports ODBC Extended Grammar",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsExtendedSQLGrammar", null);
            
            addProp(dataSet, "Supports ANSI92 Entry SQL Grammar",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsANSI92EntryLevelSQL", null);
            
            addProp(dataSet, "Supports ANSI92 Intermediate SQL Grammar",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsANSI92IntermediateSQL", null);
            
            addProp(dataSet, "Supports ANSI92 Full SQL Grammar",
                    dbMetadata.getDatabaseMetaData(), "supportsANSI92FullSQL",
                    null);
            
            addProp(dataSet, "Supports Outer Joins",
                    dbMetadata.getDatabaseMetaData(), "supportsOuterJoins",
                    null);
            
            addProp(dataSet, "Supports Full Outer Joins",
                    dbMetadata.getDatabaseMetaData(), "supportsFullOuterJoins",
                    null);
            
            addProp(dataSet, "Supports Limited Outer Joins",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsLimitedOuterJoins", null);
            
            addProp(dataSet, "Supports Positioned Delete",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsPositionedDelete", null);
            
            addProp(dataSet, "Supports Positioned Update",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsPositionedUpdate", null);
            
            addProp(dataSet, "Supports Select for Update",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsSelectForUpdate", null);
            
            addProp(dataSet, "Supports Stored Procedures",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsStoredProcedures", null);
            
            addProp(dataSet, "Supports Subqueries in Comparisons",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsSubqueriesInComparisons", null);
            
            addProp(dataSet, "Supports Subqueries in Exists",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsSubqueriesInExists", null);
            
            addProp(dataSet, "Supports Subqueries in Ins",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsSubqueriesInIns", null);
            
            addProp(dataSet, "Supports Subqueries in Quantifieds",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsSubqueriesInQuantifieds", null);
            
            addProp(dataSet, "Supports Correlated Subqueries",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsCorrelatedSubqueries", null);
            
            addProp(dataSet, "Supports Union",
                    dbMetadata.getDatabaseMetaData(), "supportsUnion", null);
            
            addProp(dataSet, "Supports Union All",
                    dbMetadata.getDatabaseMetaData(), "supportsUnionAll", null);
            
            addProp(dataSet, "Max Binary Literal Length",
                    dbMetadata.getDatabaseMetaData(),
                    "getMaxBinaryLiteralLength", null);
            
            addProp(dataSet, "Max Char Literal Length",
                    dbMetadata.getDatabaseMetaData(),
                    "getMaxCharLiteralLength", null);
            
            addProp(dataSet, "Max Column Name Length",
                    dbMetadata.getDatabaseMetaData(), "getMaxColumnNameLength",
                    null);
            
            addProp(dataSet, "Max Columns in Group By",
                    dbMetadata.getDatabaseMetaData(), "getMaxColumnsInGroupBy",
                    null);
            
            addProp(dataSet, "Max Columns in Index",
                    dbMetadata.getDatabaseMetaData(), "getMaxColumnsInIndex",
                    null);
            
            addProp(dataSet, "Max Columns in Order By",
                    dbMetadata.getDatabaseMetaData(), "getMaxColumnsInOrderBy",
                    null);
            
            addProp(dataSet, "Max Columns in Select",
                    dbMetadata.getDatabaseMetaData(), "getMaxColumnsInSelect",
                    null);
            
            addProp(dataSet, "Max Concurrent Connections",
                    dbMetadata.getDatabaseMetaData(), "getMaxConnections", null);
            
            addProp(dataSet, "Max Cursor Name Length",
                    dbMetadata.getDatabaseMetaData(), "getMaxCursorNameLength",
                    null);
            
            addProp(dataSet, "Max Index Length",
                    dbMetadata.getDatabaseMetaData(), "getMaxIndexLength", null);
            
            addProp(dataSet, "Max Schema Name Length",
                    dbMetadata.getDatabaseMetaData(), "getMaxSchemaNameLength",
                    null);
            
            addProp(dataSet, "Max Procedure Name Length",
                    dbMetadata.getDatabaseMetaData(),
                    "getMaxProcedureNameLength", null);
            
            addProp(dataSet, "Max Catalog Name Length",
                    dbMetadata.getDatabaseMetaData(),
                    "getMaxCatalogNameLength", null);
            
            addProp(dataSet, "Max Row Size", dbMetadata.getDatabaseMetaData(),
                    "getMaxRowSize", null);
            
            addProp(dataSet, "Max Statement Length",
                    dbMetadata.getDatabaseMetaData(), "getMaxStatementLength",
                    null);
            
            addProp(dataSet, "Max Statements",
                    dbMetadata.getDatabaseMetaData(), "getMaxStatements", null);
            
            addProp(dataSet, "Max Table Name Length",
                    dbMetadata.getDatabaseMetaData(), "getMaxTableNameLength",
                    null);
            
            addProp(dataSet, "Max Tables in Select",
                    dbMetadata.getDatabaseMetaData(), "getMaxTablesInSelect",
                    null);
            
            addProp(dataSet, "Max User Name Length",
                    dbMetadata.getDatabaseMetaData(), "getMaxUserNameLength",
                    null);
            
            addProp(dataSet, "Supports Transactions",
                    dbMetadata.getDatabaseMetaData(), "supportsTransactions",
                    null);
            
            addProp(dataSet, "Default Transaction Isolation",
                    dbMetadata.getDatabaseMetaData(),
                    "getDefaultTransactionIsolation", "getTransIsolation");
            
            addProp(dataSet, "Supports DDL and DML Transactions",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsDataDefinitionAndDataManipulationTransactions",
                    null);
            
            addProp(dataSet, "Supports DML Transactions Only",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsDataManipulationTransactionsOnly", null);
            
            addProp(dataSet, "Supports Batch Updates",
                    dbMetadata.getDatabaseMetaData(), "supportsBatchUpdates",
                    null);
            
            addProp(dataSet, "Supports Save Points",
                    dbMetadata.getDatabaseMetaData(), "supportsSavepoints",
                    null);
            
            addProp(dataSet, "Supports Named Parameters",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsNamedParameters", null);
            
            addProp(dataSet, "Supports Multiple Open Results",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsMultipleOpenResults", null);
            
            addProp(dataSet, "Supports Get for Auto Generated Keys",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsGetGeneratedKeys", null);
            
            addProp(dataSet, "Supports Get for Auto Generated Keys",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsGetGeneratedKeys", null);
            
            addProp(dataSet, "Supports Call Syntax",
                    dbMetadata.getDatabaseMetaData(),
                    "supportsStoredFunctionsUsingCallSyntax", null);
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
        
        return dataSet;
    }
    
    /**
     * Gets the external key columns.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the external key columns
     * @throws Exception
     *             in case of any error
     */
    public DataSet getEKColumns(String catalog, String schema, String pattern,
            String type)
        throws Exception
    
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        String tableName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        String fkName = SqlUtils.getValue(pattern, 2, METADATA_DELIMITER);
        
        ListHashMap<String, String> mapping = new ListHashMap<String, String>();
        mapping.put("FK_NAME", "FK;Foreign Key");
        mapping.put("FKTABLE_NAME", "Table");
        mapping.put("FKCOLUMN_NAME", "FK Column");
        mapping.put("PKCOLUMN_NAME", "PK Column");
        mapping.put("KEY_SEQ", "Index");
        
        dataSet = new DataSet();
        dataSet.setName("externalkeycolumns");
        dataSet.setKeyFields("FK Column");
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getExportedKeys(
                    catalog, schema, tableName);
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, false,
                    false);
            
            if (dataSet == null || dataSet.getRecordCount() == 0)
                return null;
            
            DataSetData newData = new DataSetData();
            
            ListHashMap<String, FieldDef> newFields = new ListHashMap<String, FieldDef>();
            for (int i = 1; i < dataSet.getFieldCount(); i++)
            {
                FieldDef field = dataSet.getFieldDef(i);
                
                newFields.put(field.getName(), field);
            }
            
            for (int i = 0; i < dataSet.getRecordCount(); i++)
            {
                DataSetRecord record = dataSet.getRecord(i);
                
                if (fkName.equalsIgnoreCase((String)record.get(0)))
                {
                    DataSetRecord newRecord = new DataSetRecord();
                    
                    for (int index = 1; index < record.size(); index++)
                        newRecord.add(record.get(index));
                    
                    newData.add(newRecord);
                }
            }
            
            dataSet.setData(newData);
            dataSet.setFields(newFields);
            
            return dataSet;
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
    }
    
    /**
     * Gets the external key metadata types.
     * 
     * @return the external key metadata types
     */
    public List<String> getEKMetadataTypes()
    {
        List<String> list = new ArrayList<String>();
        
        list.add(TYPE_EK_COLUMNS);
        
        return list;
    }
    
    /**
     * Gets the foreign key columns.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the foreign key columns
     * @throws Exception
     *             in case of any error
     */
    public DataSet getFKColumns(String catalog, String schema, String pattern,
            String type)
        throws Exception
    
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        String tableName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        String fkName = SqlUtils.getValue(pattern, 2, METADATA_DELIMITER);
        
        ListHashMap<String, String> mapping = new ListHashMap<String, String>();
        mapping.put("FK_NAME", "FK;Foreign Key");
        mapping.put("FKCOLUMN_NAME", "Column");
        mapping.put("PKTABLE_NAME", "Referenced Table");
        mapping.put("PKCOLUMN_NAME", "Referenced Column");
        mapping.put("KEY_SEQ", "Index");
        
        dataSet = new DataSet();
        dataSet.setName("foreignkeycolumns");
        dataSet.setKeyFields("Column");
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getImportedKeys(
                    catalog, schema, tableName);
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, false,
                    false);
            
            if (dataSet == null || dataSet.getRecordCount() == 0)
                return null;
            
            DataSetData newData = new DataSetData();
            
            ListHashMap<String, FieldDef> newFields = new ListHashMap<String, FieldDef>();
            for (int i = 1; i < dataSet.getFieldCount(); i++)
            {
                FieldDef field = dataSet.getFieldDef(i);
                
                newFields.put(field.getName(), field);
            }
            
            for (int i = 0; i < dataSet.getRecordCount(); i++)
            {
                DataSetRecord record = dataSet.getRecord(i);
                
                if (fkName.equalsIgnoreCase((String)record.get(0)))
                {
                    DataSetRecord newRecord = new DataSetRecord();
                    
                    for (int index = 1; index < record.size(); index++)
                        newRecord.add(record.get(index));
                    
                    newData.add(newRecord);
                }
            }
            
            dataSet.setData(newData);
            dataSet.setFields(newFields);
            
            return dataSet;
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
    }
    
    /**
     * Gets the foreign key definition as text.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @param name
     *            the name
     * @return the foreign key definition as text
     * @throws Exception
     *             in case of any error
     */
    public String getFkDefinitionAsText(String catalog, String schema,
            String pattern, String type, String name)
        throws Exception
    {
        pattern = pattern + METADATA_DELIMITER + name;
        
        DataSet columns = getFKColumns(catalog, schema, pattern, type);
        
        if (columns == null || columns.getRecordCount() == 0)
            return "";
        
        List<TypedKeyValue<String, Integer>> cols = new ArrayList<TypedKeyValue<String, Integer>>();
        
        for (int row = 0; row < columns.getRecordCount(); row++)
        {
            String column = (String)columns.getFieldValue(row, 0);
            String table = (String)columns.getFieldValue(row, 1);
            String pkColumn = (String)columns.getFieldValue(row, 2);
            
            Object ind = columns.getFieldValue(row, 3);
            
            int index = ind instanceof Number ? ((Number)ind).intValue()
                    : (ind instanceof String ? Integer.valueOf((String)ind)
                            : row);
            
            column = column + "," + table + "," + pkColumn;
            
            TypedKeyValue<String, Integer> value = new TypedKeyValue<String, Integer>(
                    column, index);
            
            cols.add(value);
        }
        
        Collections.sort(cols, new Comparator<TypedKeyValue<String, Integer>>()
        {
            public int compare(TypedKeyValue<String, Integer> o1,
                    TypedKeyValue<String, Integer> o2)
            {
                return o1.getValue().compareTo(o2.getValue());
            }
            
        });
        
        String fields = "";
        String pkFields = "";
        String table = "";
        for (int i = 0; i < cols.size(); i++)
        {
            String key = cols.get(i).getKey();
            
            String[] values = key.split(",", -1);
            
            String column = values[0];
            table = values[1];
            String pkColumn = values[2];
            
            fields = fields + column;
            
            if (i < cols.size() - 1)
                fields = fields + ",";
            
            pkFields = pkFields + pkColumn;
            
            if (i < cols.size() - 1)
                pkFields = pkFields + ",";
            
        }
        
        return "    CONSTRAINT " + name + " FOREIGN KEY (" + fields
                + ") REFERENCES " + table + " (" + pkFields + ")";
    }
    
    /**
     * Gets the foreign key metadata types.
     * 
     * @return the foreign key metadata types
     */
    public List<String> getFKMetadataTypes()
    {
        List<String> list = new ArrayList<String>();
        
        list.add(TYPE_FK_COLUMNS);
        
        return list;
    }
    
    /**
     * Gets the foreign key as text.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the foreign key as text
     * @throws Exception
     *             in case of any error
     */
    public String getFksAsText(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        DataSet fks = null;
        
        try
        {
            fks = getTableFKs(catalog, schema, pattern, type);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this, Resource.ERROR_GENERAL.getValue(),
                    ex);
            
            return null;
        }
        
        if (fks == null || fks.getRecordCount() == 0)
            return "";
        
        String allFks = "";
        
        for (int row = 0; row < fks.getRecordCount(); row++)
        {
            String fk = getFkDefinitionAsText(catalog, schema, pattern, type,
                    (String)fks.getFieldValue(row, 0));
            
            if (Utils.isNothing(fk))
                continue;
            
            allFks = allFks + fk;
            
            if (row < fks.getRecordCount() - 1)
                allFks = allFks + ",\n";
        }
        
        return allFks;
    }
    
    /**
     * Gets the function metadata types.
     * 
     * @return the function metadata types
     */
    public List<String> getFunctMetadataTypes()
    {
        List<String> list = new ArrayList<String>();
        
        list.add(TYPE_FUNCT_PARAMETERS);
        
        return list;
    }
    
    /**
     * Gets the function parameters.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the function parameters
     * @throws Exception
     *             in case of any error
     */
    public DataSet getFunctParameters(String catalog, String schema,
            String pattern, String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        String procName = SqlUtils.getValue(pattern, 2, METADATA_DELIMITER);
        if (Utils.isNothing(procName))
        {
            procName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        }
        else
        {
            if (Utils.isNothing(catalog))
                catalog = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        }
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getFunctionColumns(
                    catalog, schema, procName, null);
            
            ListHashMap<String, String> mapping = new ListHashMap<String, String>();
            mapping.put("COLUMN_NAME", "Name");
            mapping.put("COLUMN_TYPE", "Parameter Type");
            mapping.put("DATA_TYPE", "JDBC Type");
            mapping.put("TYPE_NAME", "Sql Type");
            mapping.put("PRECISION", "Precision");
            mapping.put("SCALE", "Scale");
            mapping.put("NULLABLE", "Nullable");
            mapping.put("ORDINAL_POSITION", "Position");
            
            dataSet = new DataSet();
            dataSet.setName("functparams");
            dataSet.setKeyFields("Name");
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, true,
                    true);
            
            Map<String, TypedKeyValue<String, String>> displayFunctions = new HashMap<String, TypedKeyValue<String, String>>();
            
            displayFunctions.put(
                    "Parameter Type",
                    new TypedKeyValue<String, String>(JdbcMetadata.class
                            .getName(), "getFuncParamType"));
            
            displayFunctions.put("JDBC Type",
                    new TypedKeyValue<String, String>(SqlUtils.class.getName(),
                            "getTypeName"));
            
            dataSet.setDisplayFunctions(displayFunctions);
            
            return dataSet;
        }
        catch (Throwable ex)
        {
            return getProcParameters(catalog, schema, pattern, type);
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
    }
    
    /**
     * Gets the functions.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the function
     * @throws Exception
     *             in case of any error
     */
    public DataSet getFuncts(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getFunctions(
                    catalog, schema, null);
            
            ListHashMap<String, String> mapping = new ListHashMap<String, String>();
            mapping.put("FUNCTION_CAT", "Catalog");
            mapping.put("FUNCTION_SCHEM", "Schema");
            mapping.put("FUNCTION_NAME", "Name");
            mapping.put("FUNCTION_TYPE", "Return Type");
            
            dataSet = new DataSet();
            dataSet.setName("functions");
            dataSet.setKeyFields("Name");
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, false,
                    true);
            
            Map<String, TypedKeyValue<String, String>> displayFunctions = new HashMap<String, TypedKeyValue<String, String>>();
            
            displayFunctions.put(
                    "Return Type",
                    new TypedKeyValue<String, String>(JdbcMetadata.class
                            .getName(), "getFuncRetType"));
            
            dataSet.setDisplayFunctions(displayFunctions);
            
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
        
        return dataSet;
    }
    
    /**
     * Gets the index columns.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the index columns
     * @throws Exception
     *             in case of any error
     */
    public DataSet getIndexColumns(String catalog, String schema,
            String pattern, String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        String tableName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        String indexName = SqlUtils.getValue(pattern, 2, METADATA_DELIMITER);
        if (Utils.isNothing(indexName))
        {
            indexName = tableName;
            tableName = null;
        }
        
        ListHashMap<String, String> mapping = new ListHashMap<String, String>();
        mapping.put("INDEX_NAME", "Index");
        mapping.put("COLUMN_NAME", "Column");
        mapping.put("ASC_OR_DESC", "Asc or Desc");
        
        dataSet = new DataSet();
        dataSet.setName("indexcolumns");
        dataSet.setKeyFields("Column");
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getIndexInfo(
                    catalog, schema, tableName, false, false);
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, false,
                    false);
            
            if (dataSet == null || dataSet.getRecordCount() == 0)
                return null;
            
            DataSetData newData = new DataSetData();
            
            ListHashMap<String, FieldDef> newFields = new ListHashMap<String, FieldDef>();
            for (int i = 1; i < dataSet.getFieldCount(); i++)
            {
                FieldDef field = dataSet.getFieldDef(i);
                
                newFields.put(field.getName(), field);
            }
            
            for (int i = 0; i < dataSet.getRecordCount(); i++)
            {
                DataSetRecord record = dataSet.getRecord(i);
                
                if (indexName.equalsIgnoreCase((String)record.get(0)))
                {
                    DataSetRecord newRecord = new DataSetRecord();
                    
                    for (int index = 1; index < record.size(); index++)
                        newRecord.add(record.get(index));
                    
                    newData.add(newRecord);
                }
            }
            
            dataSet.setData(newData);
            dataSet.setFields(newFields);
            
            return dataSet;
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
    }
    
    /**
     * Gets the index definition as text.
     *
     * @param catalog the catalog
     * @param schema the schema
     * @param pattern the pattern
     * @param type the type
     * @param tableName the table name
     * @param name the name
     * @param unique if true the "unique" is added before index name
     * @param suffix the suffix will be added to the index name
     * @param toUpper if true converts fields names to upper case
     * @param leadingNumberNotAllowed the leading number not allowed
     * @return the index definition as text
     * @throws Exception in case of any error
     */
    public String getIndexDefinitionAsText(String catalog, String schema,
            String pattern, String type, String tableName, String name,
            boolean unique, String suffix, boolean toUpper,
            boolean leadingNumberNotAllowed)
        throws Exception
    {
        pattern = pattern + METADATA_DELIMITER + name;
        
        DataSet columns = getIndexColumns(catalog, schema, pattern, type);
        
        if (columns == null || columns.getRecordCount() == 0)
            return "";
        
        String fields = "";
        
        for (int row = 0; row < columns.getRecordCount(); row++)
        {
            String column = (String)columns.getFieldValue(row, 0);
            column = toUpper ? column.toUpperCase() : column;
            String askDesc = Utils.makeString(columns.getFieldValue(row, 1))
                    .trim();
            
            boolean isDesc = askDesc.indexOf("D") == 0;
            
            fields = fields + column + (isDesc ? " DESC" : "");
            
            if (row < columns.getRecordCount() - 1)
                fields = fields + ",";
        }
        
        String uniqueStr = unique ? " UNIQUE INDEX " : " INDEX ";
        
        if (leadingNumberNotAllowed && Utils.isNumber(name.substring(0, 1)))
        {
            name = "i" + name;
        }
        
        return "CREATE" + uniqueStr + name + Utils.makeString(suffix) + " ON "
                + tableName + " (" + fields + ")";
    }
    
    /**
     * Gets the indexes as text.
     *
     * @param catalog the catalog
     * @param schema the schema
     * @param pattern the pattern
     * @param type the type
     * @param tName the table name to replace original table name
     * @param suffix will be added to the end of the index name
     * @param toUpper if true converts fields names to upper case
     * @param leadingNumberNotAllowed the leading number not allowed
     * @return the indexes as text
     * @throws Exception in case of any error
     */
    public String getIndexesAsText(String catalog, String schema,
            String pattern, String type, String tName, String suffix,
            boolean toUpper, boolean leadingNumberNotAllowed)
        throws Exception
    {
        DataSet indexes = null;
        
        try
        {
            indexes = getTableIndexes(catalog, schema, pattern, type);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this, Resource.ERROR_GENERAL.getValue(),
                    ex);
            
            return null;
        }
        
        if (indexes == null || indexes.getRecordCount() == 0)
            return "";
        
        String tableName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        
        String allIndexes = "";
        
        for (int row = 0; row < indexes.getRecordCount(); row++)
        {
            String index = getIndexDefinitionAsText(catalog, schema, pattern,
                    type, Utils.isNothing(tName) ? tableName : tName,
                    (String)indexes.getFieldValue(row, 0),
                    isUnique(indexes, row), suffix, toUpper,
                    leadingNumberNotAllowed);
            
            if (Utils.isNothing(index))
                continue;
            
            allIndexes = allIndexes + index;
            
            if (row < indexes.getRecordCount() - 1)
                allIndexes = allIndexes + ";\n";
            else
                allIndexes = allIndexes + ";";
        }
        
        return allIndexes;
    }
    
    /**
     * Gets the index metadata types.
     * 
     * @return the index metadata types
     */
    public List<String> getIndexMetadataTypes()
    {
        List<String> list = new ArrayList<String>();
        
        list.add(TYPE_INDEX_COLUMNS);
        
        return list;
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
     * @see com.toolsverse.etl.metadata.BaseMetadata#getMetadataMethods()
     */
    @Override
    public Map<String, String> getMetadataMethods()
    {
        return METADATA_METHODS;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#getName()
     */
    public String getName()
    {
        return NAME;
    }
    
    /**
     * Gets the owners.
     * 
     * @return the owners
     * @throws Exception
     *             in case of any error
     */
    public List<Object> getOwners()
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        try
        {
            dbMetadata = getDbMetadata();
            
            setHasCatalogs(dbMetadata.getDatabaseMetaData()
                    .supportsCatalogsInDataManipulation());
            setHasSchemas(dbMetadata.getDatabaseMetaData()
                    .supportsSchemasInTableDefinitions());
            
            if (hasCatalogs())
            {
                setCurrentDatabase(dbMetadata.getConnection().getCatalog());
                
                return getCatalogs(hasSchemas());
            }
            else
            {
                setCurrentDatabase(dbMetadata.getDatabaseMetaData()
                        .getUserName());
                
                return getSchemas(null);
            }
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
    }
    
    /**
     * Gets the parameters.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the parameters
     * @throws Exception
     *             in case of any error
     */
    public DataSet getParameters(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        return getProcParameters(catalog, schema, pattern, type);
    }
    
    /**
     * Gets the primary key as text.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the primary key as text
     * @throws Exception
     *             in case of any error
     */
    public String getPkAsText(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        DataSet pk = null;
        
        try
        {
            pk = getTablePKs(catalog, schema, pattern, type);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this, Resource.ERROR_GENERAL.getValue(),
                    ex);
            
            return null;
        }
        
        if (pk == null || pk.getRecordCount() == 0)
            return "";
        
        String name = (String)pk.getFieldValue(0, 0);
        
        pattern = pattern + METADATA_DELIMITER + name;
        
        DataSet columns = getPKColumns(catalog, schema, pattern, type);
        
        if (columns == null || columns.getRecordCount() == 0)
            return "";
        
        List<TypedKeyValue<String, Integer>> cols = new ArrayList<TypedKeyValue<String, Integer>>();
        
        for (int row = 0; row < columns.getRecordCount(); row++)
        {
            String column = (String)columns.getFieldValue(row, 0);
            
            Object ind = columns.getFieldValue(row, 1);
            
            int index = ind instanceof Number ? ((Number)ind).intValue()
                    : (ind instanceof String ? Integer.valueOf((String)ind)
                            : row);
            
            TypedKeyValue<String, Integer> value = new TypedKeyValue<String, Integer>(
                    column, index);
            
            cols.add(value);
        }
        
        Collections.sort(cols, new Comparator<TypedKeyValue<String, Integer>>()
        {
            public int compare(TypedKeyValue<String, Integer> o1,
                    TypedKeyValue<String, Integer> o2)
            {
                return o1.getValue().compareTo(o2.getValue());
            }
            
        });
        
        String fields = "";
        for (int i = 0; i < cols.size(); i++)
        {
            fields = fields + cols.get(i).getKey();
            
            if (i < cols.size() - 1)
                fields = fields + ",";
        }
        
        return "    CONSTRAINT " + name + " PRIMARY KEY (" + fields + ")";
    }
    
    /**
     * Gets the primary key columns.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the primary key columns
     * @throws Exception
     *             in case of any error
     */
    public DataSet getPKColumns(String catalog, String schema, String pattern,
            String type)
        throws Exception
    
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        String tableName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        String pkName = SqlUtils.getValue(pattern, 2, METADATA_DELIMITER);
        
        ListHashMap<String, String> mapping = new ListHashMap<String, String>();
        mapping.put("PK_NAME", "PK;Primary Key");
        mapping.put("COLUMN_NAME", "Column");
        mapping.put("KEY_SEQ", "Index");
        
        dataSet = new DataSet();
        dataSet.setName("primarykeycolumns");
        dataSet.setKeyFields("Column");
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getPrimaryKeys(
                    catalog, schema, tableName);
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, false,
                    false);
            
            if (dataSet == null || dataSet.getRecordCount() == 0)
                return null;
            
            DataSetData newData = new DataSetData();
            
            ListHashMap<String, FieldDef> newFields = new ListHashMap<String, FieldDef>();
            for (int i = 1; i < dataSet.getFieldCount(); i++)
            {
                FieldDef field = dataSet.getFieldDef(i);
                
                newFields.put(field.getName(), field);
            }
            
            for (int i = 0; i < dataSet.getRecordCount(); i++)
            {
                DataSetRecord record = dataSet.getRecord(i);
                
                if (pkName == null
                        || pkName.equalsIgnoreCase((String)record.get(0)))
                {
                    DataSetRecord newRecord = new DataSetRecord();
                    
                    for (int index = 1; index < record.size(); index++)
                        newRecord.add(record.get(index));
                    
                    newData.add(newRecord);
                }
            }
            
            dataSet.setData(newData);
            dataSet.setFields(newFields);
            
            return dataSet;
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
    }
    
    /**
     * Gets the primary key metadata types.
     * 
     * @return the primary key metadata types
     */
    public List<String> getPKMetadataTypes()
    {
        List<String> list = new ArrayList<String>();
        
        list.add(TYPE_PK_COLUMNS);
        
        return list;
    }
    
    /**
     * Gets the procedure metadata types.
     * 
     * @return the procedure metadata types
     */
    public List<String> getProcMetadataTypes()
    {
        List<String> list = new ArrayList<String>();
        
        list.add(TYPE_PROC_PARAMETERS);
        
        return list;
    }
    
    /**
     * Gets the procedure parameters.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the procedure parameters
     * @throws Exception
     *             in case of any error
     */
    public DataSet getProcParameters(String catalog, String schema,
            String pattern, String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        String procName = SqlUtils.getValue(pattern, 2, METADATA_DELIMITER);
        if (Utils.isNothing(procName))
        {
            procName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        }
        else
        {
            if (Utils.isNothing(catalog))
                catalog = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        }
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData()
                    .getProcedureColumns(catalog, schema, procName, null);
            
            ListHashMap<String, String> mapping = new ListHashMap<String, String>();
            mapping.put("COLUMN_NAME", "Name");
            mapping.put("COLUMN_TYPE", "Parameter Type");
            mapping.put("DATA_TYPE", "JDBC Type");
            mapping.put("TYPE_NAME", "Sql Type");
            mapping.put("PRECISION", "Precision");
            mapping.put("SCALE", "Scale");
            mapping.put("COLUMN_DEF", "Default");
            mapping.put("NULLABLE", "Nullable");
            mapping.put("ORDINAL_POSITION", "Position");
            
            dataSet = new DataSet();
            dataSet.setName("procparams");
            dataSet.setKeyFields("Name");
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, true,
                    true);
            
            Map<String, TypedKeyValue<String, String>> displayFunctions = new HashMap<String, TypedKeyValue<String, String>>();
            
            displayFunctions.put(
                    "Parameter Type",
                    new TypedKeyValue<String, String>(JdbcMetadata.class
                            .getName(), "getFuncParamType"));
            
            displayFunctions.put("JDBC Type",
                    new TypedKeyValue<String, String>(SqlUtils.class.getName(),
                            "getTypeName"));
            
            dataSet.setDisplayFunctions(displayFunctions);
            
            return dataSet;
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
    }
    
    /**
     * Gets the procedures.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the procedures
     * @throws Exception
     *             in case of any error
     */
    public DataSet getProcs(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getProcedures(
                    catalog, schema, null);
            
            ListHashMap<String, String> mapping = new ListHashMap<String, String>();
            mapping.put("PROCEDURE_CAT", "Catalog");
            mapping.put("PROCEDURE_SCHEM", "Schema");
            mapping.put("PROCEDURE_NAME", "Name");
            mapping.put("PROCEDURE_TYPE", "Return Type");
            
            dataSet = new DataSet();
            dataSet.setName("procedures");
            dataSet.setKeyFields("Name");
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, false,
                    true);
            
            Map<String, TypedKeyValue<String, String>> displayFunctions = new HashMap<String, TypedKeyValue<String, String>>();
            
            displayFunctions.put(
                    "Return Type",
                    new TypedKeyValue<String, String>(JdbcMetadata.class
                            .getName(), "getProcsRetType"));
            
            dataSet.setDisplayFunctions(displayFunctions);
            
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
        
        return dataSet;
    }
    
    /**
     * Gets the property value.
     * 
     * @param metaData
     *            the meta data
     * @param func
     *            the function
     * @param convert
     *            the convert
     * @return the prop value
     */
    private String getPropValue(DatabaseMetaData metaData, String func,
            String convert)
    {
        try
        {
            Method invokeMethod = DatabaseMetaData.class.getMethod(func,
                    (Class[])null);
            
            Object value = invokeMethod.invoke(metaData, (Object[])null);
            
            if (value != null)
            {
                if (!Utils.isNothing(convert))
                    try
                    {
                        invokeMethod = getClass().getMethod(convert,
                                new Class[] {Object.class});
                        
                        value = invokeMethod.invoke(this, new Object[] {value});
                    }
                    catch (Throwable th)
                    {
                        Logger.log(Logger.SEVERE, this,
                                Resource.ERROR_GENERAL.getValue(), th);
                    }
                
                return value.toString();
                
            }
        }
        catch (Throwable ex)
        {
            Logger.log(Logger.SEVERE, this, Resource.ERROR_GENERAL.getValue(),
                    ex);
            
            return null;
        }
        
        return null;
    }
    
    /**
     * Gets the schemas.
     * 
     * @param catalog
     *            the catalog
     * @return the schemas
     * @throws Exception
     *             in case of any error
     */
    public List<Object> getSchemas(String catalog)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        ResultSet rs = null;
        String oldCatalog = null;
        List<Object> list = new ArrayList<Object>();
        
        try
        {
            dbMetadata = getDbMetadata();
            
            if (!Utils.isNothing(catalog))
            {
                oldCatalog = dbMetadata.getConnection().getCatalog();
                
                dbMetadata.getConnection().setCatalog(catalog);
            }
            
            rs = dbMetadata.getDatabaseMetaData().getSchemas();
            String currentSchema = null;
            
            while (rs.next())
            {
                String schema = rs.getString("TABLE_SCHEM");
                
                if (Utils.isNothing(catalog)
                        && !Utils.isNothing(getCurrentDatabase())
                        && isDatabaseCurrent(schema))
                    currentSchema = schema;
                else
                    list.add(new KeyValue(schema, null));
            }
            
            if (currentSchema != null)
                list.add(0, new KeyValue(currentSchema, null));
        }
        finally
        {
            if (rs != null)
                rs.close();
            
            if (!Utils.isNothing(catalog) && dbMetadata != null)
                dbMetadata.getConnection().setCatalog(oldCatalog);
            
            if (dbMetadata != null)
                dbMetadata.free();
            
        }
        
        return list;
    }
    
    /**
     * Gets the table as text.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the table as text
     * @throws Exception
     *             in case of any error
     */
    public String getTableAsText(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        String tableName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        
        if (Utils.isNothing(tableName))
            tableName = SqlUtils.getValue(pattern, 0, METADATA_DELIMITER);
        
        if (Utils.isNothing(tableName))
            return null;
        
        DataSet columns = getTableColumns(catalog, schema, pattern, type);
        
        if (columns == null || columns.getRecordCount() == 0)
            return null;
        
        String[] values = getTableNameAsText(catalog, schema, pattern, type,
                tableName).split(";", -1);
        
        String create = "CREATE " + values[0] + "\n(\n";
        
        String fields = "";
        
        for (int row = 0; row < columns.getRecordCount(); row++)
        {
            String name = (String)columns.getFieldValue(row, 0);
            String fType = (String)columns.getFieldValue(row, 1);
            Object size = columns.getFieldValue(row, 2);
            Object decimal = columns.getFieldValue(row, 3);
            Object nullable = columns.getFieldValue(row, 4);
            Number javaType = (Number)columns.getFieldValue(row, 5);
            
            fields = fields
                    + "    "
                    + SqlUtils.getFieldAsText(getDriver(), name, fType, size,
                            decimal, nullable, javaType.intValue());
            
            if (row < columns.getRecordCount() - 1)
                fields = fields + ",\n";
        }
        
        String pk = getPkAsText(catalog, schema, pattern, type);
        
        if (!Utils.isNothing(pk))
            fields = fields + ",\n" + pk;
        
        String fks = getFksAsText(catalog, schema, pattern, type);
        
        if (!Utils.isNothing(fks))
            fields = fields + ",\n" + fks + "\n";
        else
            fields = fields + "\n";
        
        create = create + fields + ")" + (values.length > 1 ? values[1] : "")
                + ";";
        
        String indexes = getIndexesAsText(catalog, schema, pattern, type, null,
                null, false, false);
        
        if (!Utils.isNothing(indexes))
            create = create + "\n" + indexes;
        
        return create;
    }
    
    /**
     * Gets the table columns.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the table columns
     * @throws Exception
     *             in case of any error
     */
    public DataSet getTableColumns(String catalog, String schema,
            String pattern, String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        String tableName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getColumns(catalog,
                    schema, tableName, null);
            
            ListHashMap<String, String> mapping = new ListHashMap<String, String>();
            mapping.put("COLUMN_NAME", "Name");
            mapping.put("TYPE_NAME", "Type");
            mapping.put("COLUMN_SIZE", "Size");
            mapping.put("DECIMAL_DIGITS", "Decimal");
            mapping.put("IS_NULLABLE", "Nullable");
            mapping.put("DATA_TYPE", "JDBC Type");
            mapping.put("IS_AUTOINCREMENT", "Autoincrement");
            
            dataSet = new DataSet();
            dataSet.setName("tablecolumns");
            dataSet.setKeyFields("Name");
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, true,
                    true);
            
            if (getDriver().getWrongScale() != -1)
            {
                int index = dataSet.getFieldIndex("Decimal");
                
                int typeIndex = dataSet.getFieldIndex("JDBC Type");
                
                if (index >= 0 && typeIndex >= 0)
                {
                    for (int row = 0; row < dataSet.getRecordCount(); row++)
                    {
                        
                        Object jdbcTypeOb = dataSet.getFieldValue(row,
                                typeIndex);
                        
                        if (jdbcTypeOb != null)
                        {
                            try
                            {
                                int jdbcType = Integer.parseInt(jdbcTypeOb
                                        .toString());
                                
                                if (SqlUtils.isNumber(jdbcType))
                                {
                                    Object decOb = dataSet.getFieldValue(row,
                                            index);
                                    
                                    if (decOb != null)
                                        try
                                        {
                                            int dec = Integer.parseInt(decOb
                                                    .toString());
                                            
                                            if (dec == getDriver()
                                                    .getWrongScale())
                                                dataSet.setFieldValue(row,
                                                        index, 0);
                                            
                                        }
                                        catch (Exception ex)
                                        {
                                            // ignore
                                        }
                                    
                                }
                            }
                            catch (Exception ex)
                            {
                                // ignore
                            }
                        }
                    }
                }
            }
            
            Map<String, TypedKeyValue<String, String>> displayFunctions = new HashMap<String, TypedKeyValue<String, String>>();
            
            displayFunctions.put("JDBC Type",
                    new TypedKeyValue<String, String>(SqlUtils.class.getName(),
                            "getTypeName"));
            
            dataSet.setDisplayFunctions(displayFunctions);
            
            return dataSet;
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
    }
    
    /**
     * Gets the table external keys.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the table external keys
     * @throws Exception
     *             in case of any error
     */
    public DataSet getTableEKs(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        String tableName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        
        ListHashMap<String, String> mapping = new ListHashMap<String, String>();
        mapping.put("FK_NAME", "Name;External Key");
        mapping.put("FKTABLE_NAME", "Table");
        mapping.put("UPDATE_RULE", "On Update");
        mapping.put("DELETE_RULE", "On Delete");
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getExportedKeys(
                    catalog, schema, tableName);
            
            dataSet = new DataSet();
            dataSet.setName("tableexternalkeys");
            dataSet.setKeyFields("Name");
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, true,
                    true);
            
            return dataSet;
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
    }
    
    /**
     * Gets the table foreign keys.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the table foreign keys
     * @throws Exception
     *             in case of any error
     */
    public DataSet getTableFKs(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        String tableName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        
        ListHashMap<String, String> mapping = new ListHashMap<String, String>();
        mapping.put("FK_NAME", "Name;Foreign Key");
        mapping.put("UPDATE_RULE", "On Update");
        mapping.put("DELETE_RULE", "On Delete");
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getImportedKeys(
                    catalog, schema, tableName);
            
            dataSet = new DataSet();
            dataSet.setName("tableforeignkeys");
            dataSet.setKeyFields("Name");
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, true,
                    true);
            
            return dataSet;
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
    }
    
    /**
     * Gets the table indexes.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the table indexes
     * @throws Exception
     *             in case of any error
     */
    public DataSet getTableIndexes(String catalog, String schema,
            String pattern, String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        String tableName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        
        ListHashMap<String, String> mapping = new ListHashMap<String, String>();
        mapping.put("INDEX_NAME", "Name");
        mapping.put("NON_UNIQUE", "Unique");
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getIndexInfo(
                    catalog, schema, tableName, false, false);
            
            dataSet = new DataSet();
            dataSet.setName("tableindexes");
            dataSet.setKeyFields("Name");
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, true,
                    true);
            
            for (int row = 0; row < dataSet.getRecordCount(); row++)
            {
                boolean isUnique = false;
                
                Object unique = dataSet.getFieldValue(row, 1);
                
                if (unique != null)
                {
                    isUnique = unique instanceof Boolean ? !(Boolean)unique
                            : !Utils.str2Boolean(unique.toString(), false);
                }
                
                dataSet.setFieldValue(row, 1, isUnique);
            }
            
            return dataSet;
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
    }
    
    /**
     * Gets the table metadata types.
     * 
     * @return the table metadata types
     */
    public List<String> getTableMetadataTypes()
    {
        List<String> list = new ArrayList<String>();
        
        list.add(TYPE_COLUMNS);
        list.add(TYPE_INDEXES);
        list.add(TYPE_PKS);
        list.add(TYPE_FKS);
        list.add(TYPE_EKS);
        
        return list;
    }
    
    /**
     * Gets the table name as text.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @param name
     *            the name
     * @return the table name as text
     * @throws Exception
     *             in case of any error
     */
    public String getTableNameAsText(String catalog, String schema,
            String pattern, String type, String name)
        throws Exception
    {
        return "TABLE " + name;
    }
    
    /**
     * Gets the table primary key.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the table primary key
     * @throws Exception
     *             in case of any error
     */
    public DataSet getTablePKs(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        String tableName = SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
        
        ListHashMap<String, String> mapping = new ListHashMap<String, String>();
        mapping.put("PK_NAME", "Name;Primary Key");
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getPrimaryKeys(
                    catalog, schema, tableName);
            
            dataSet = new DataSet();
            dataSet.setName("tableprimarykeys");
            dataSet.setKeyFields("Name");
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, true,
                    true);
            
            return dataSet;
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
    }
    
    /**
     * Gets the tables by type.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the tables by type
     * @throws Exception
     *             in case of any error
     */
    public DataSet getTablesByType(String catalog, String schema,
            String pattern, String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        try
        {
            String[] types = {type};
            
            if (type == null)
                types = null;
            
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getTables(catalog,
                    schema, null, types);
            
            ListHashMap<String, String> mapping = new ListHashMap<String, String>();
            mapping.put("TABLE_CAT", "Catalog");
            mapping.put("TABLE_SCHEM", "Schema");
            mapping.put("TABLE_NAME", "Name");
            mapping.put("TABLE_TYPE", "Type");
            
            dataSet = new DataSet();
            dataSet.setName(TABLES_DATASET_TYPE);
            dataSet.setKeyFields("Name");
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, false,
                    true);
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
        
        return dataSet;
    }
    
    /**
     * Gets the table types.
     * 
     * @return the table types
     * @throws Exception
     *             in case of any error
     */
    public List<String> getTableTypes()
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        List<String> list = new ArrayList<String>();
        
        ResultSet rs = null;
        
        try
        {
            dbMetadata = getDbMetadata();
            
            rs = dbMetadata.getDatabaseMetaData().getTableTypes();
            
            while (rs.next())
            {
                String tType = Utils.makeString(rs.getString("TABLE_TYPE"))
                        .trim();
                
                if (!list.contains(tType))
                    list.add(tType);
            }
        }
        finally
        {
            if (rs != null)
                rs.close();
            
            if (dbMetadata != null)
                dbMetadata.free();
        }
        
        return list;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#getTopLevelDbObjects()
     */
    public List<Object> getTopLevelDbObjects()
        throws Exception
    {
        List<Object> objects = getOwners();
        
        objects.add(0, TYPE_DBINFO);
        objects.add(1, TYPE_TYPES);
        
        return objects;
    }
    
    /**
     * Gets the transaction isolation level.
     * 
     * @param value
     *            the value
     * @return the transaction isolation level
     */
    public String getTransIsolation(final Object value)
    {
        int val = (Integer)value;
        
        switch (val)
        {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                return "Read Uncommitted";
            case Connection.TRANSACTION_READ_COMMITTED:
                return "Read Committed";
            case Connection.TRANSACTION_REPEATABLE_READ:
                return "Repeatable Read";
            case Connection.TRANSACTION_SERIALIZABLE:
                return "Serializable";
            case Connection.TRANSACTION_NONE:
                return "None";
            default:
                return "None";
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#getTypeMethods()
     */
    public Map<String, String> getTypeMethods()
    {
        return TYPE_METHODS;
    }
    
    /**
     * Gets the types.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the types
     * @throws Exception
     *             in case of any error
     */
    public DataSet getTypes(String catalog, String schema, String pattern,
            String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        DataSet dataSet = null;
        
        try
        {
            dbMetadata = getDbMetadata();
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getTypeInfo();
            
            ListHashMap<String, String> mapping = new ListHashMap<String, String>();
            mapping.put("TYPE_NAME", "Name");
            mapping.put("DATA_TYPE", "JDBC Type");
            mapping.put("PRECISION", "Maximum precision");
            mapping.put("LITERAL_PREFIX", "Literal Prefix");
            mapping.put("LITERAL_SUFFIX", "Literal Suffix");
            mapping.put("CREATE_PARAMS", "Create params");
            mapping.put("NULLABLE", "Nullable");
            mapping.put("CASE_SENSITIVE", "Case Sensitive");
            mapping.put("SEARCHABLE", "Searchable");
            mapping.put("UNSIGNED_ATTRIBUTE", "Unsigned");
            mapping.put("FIXED_PREC_SCALE", "Can be Money");
            mapping.put("AUTO_INCREMENT", "Auto Incerement");
            mapping.put("LOCAL_TYPE_NAME", "Localized Name");
            mapping.put("MINIMUM_SCALE", "Min Scale");
            mapping.put("MAXIMUM_SCALE", "Max Scale");
            mapping.put("NUM_PREC_RADIX", "Radix");
            
            dataSet = new DataSet();
            dataSet.setName("datatypes");
            dataSet.setKeyFields("Name");
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, false,
                    true);
            
            Map<String, TypedKeyValue<String, String>> displayFunctions = new HashMap<String, TypedKeyValue<String, String>>();
            
            displayFunctions.put("JDBC Type",
                    new TypedKeyValue<String, String>(SqlUtils.class.getName(),
                            "getTypeName"));
            
            dataSet.setDisplayFunctions(displayFunctions);
            
        }
        finally
        {
            if (dbMetadata != null)
                dbMetadata.free();
        }
        
        return dataSet;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.BaseMetadata#getTypesByParent()
     */
    @Override
    public Map<String, String> getTypesByParent()
    {
        return TYPES_BY_PARENT;
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
    
    /**
     * Gets the view metadata types.
     * 
     * @return the view metadata types
     */
    public List<String> getViewMetadataTypes()
    {
        List<String> list = new ArrayList<String>();
        
        list.add(TYPE_COLUMNS);
        
        return list;
    }
    
    /**
     * Checks if index is unique using given data set with indexes and row which is a number of the index.
     * Can be database specific.
     * 
     * @param dataSet the data set with indexes
     * @param row the a number of the index
     * @return true if index is unique
     */
    public boolean isUnique(DataSet dataSet, int row)
    {
        int findex = dataSet.getFieldIndex("Unique");
        
        Object unique = null;
        boolean isUnique = false;
        
        if (findex >= 0)
        {
            unique = dataSet.getFieldValue(row, findex);
        }
        else
            unique = dataSet.getFieldValue(row, 1);
        
        isUnique = unique instanceof Boolean ? (Boolean)unique : (unique
                .toString().trim().toLowerCase().indexOf("unique") == 0 ? true
                : (Utils.str2Boolean(unique.toString(), false)));
        
        return isUnique;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.BaseMetadata#supportsAsText(java.lang.String)
     */
    @Override
    public boolean supportsAsText(String type)
    {
        return TYPE_SUPPORT_AS_TEXT.containsKey(type);
    }
    
}