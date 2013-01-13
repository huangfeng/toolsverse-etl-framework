/*
 * Metadata.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.metadata;

import java.util.List;
import java.util.Map;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.ConnectionParamsProvider;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.sql.connection.ConnectionFactory;
import com.toolsverse.ext.ExtensionModule;

/**
 * This interface defines generic methods to retrieve the metadata for the
 * database objects, such as tables, views, indexes, etc.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface Metadata extends ExtensionModule
{
    
    /** The base class path. */
    static final String BASE_CLASS_PATH = "com.toolsverse.etl.metadata";
    
    /** The TYPE_CATALOG. */
    static final String TYPE_CATALOG = "CATALOG";
    
    /** The TYPE_SCHEMA. */
    static final String TYPE_SCHEMA = "SCHEMA";
    
    /** The TYPE_TYPES. */
    static final String TYPE_TYPES = "Database Types";
    
    /** The TYPE_DBINFO. */
    static final String TYPE_DBINFO = "Database Info";
    
    /** The TYPE_DBPROPS. */
    static final String TYPE_DBPROPS = "Properties";
    
    /** The TYPE_DBPROP. */
    static final String TYPE_DBPROP = "Property";
    
    /** The TYPE_DBFUNCTIONS. */
    static final String TYPE_DBFUNCTIONS = "Keywords and Functions";
    
    /** The TYPE_DBFUNCT. */
    static final String TYPE_DBFUNCT = "Database Function";
    
    /** The TYPE_FUNCTS. */
    static final String TYPE_FUNCTS = "Functions";
    
    /** The TYPE_FUNCT. */
    static final String TYPE_FUNCT = "Function";
    
    /** The TYPE_PROCS. */
    static final String TYPE_PROCS = "Procedures";
    
    /** The TYPE_PROC. */
    static final String TYPE_PROC = "Procedure";
    
    /** The TYPE_EXEC_METHODS. */
    static final String TYPE_EXEC_METHODS = "Methods";
    
    /** The TYPE_EXEC_METHOD. */
    static final String TYPE_EXEC_METHOD = "Method";
    
    /** The TYPE_PROCS_FUNCS. */
    static final String TYPE_PROCS_FUNCS = "Procedures and Functions";
    
    /** The TYPE_PACKAGES. */
    static final String TYPE_PACKAGES = "Packages";
    
    /** The TYPE_PACKAGE. */
    static final String TYPE_PACKAGE = "Package";
    
    /** The TYPE_ASSEMBLIES. */
    static final String TYPE_ASSEMBLIES = "Assemblies";
    
    /** The TYPE_ASSEMBLY. */
    static final String TYPE_ASSEMBLY = "Assembly";
    
    /** The TYPE_TRIGGERS. */
    static final String TYPE_TRIGGERS = "Triggers";
    
    /** The TYPE_TRIGGER. */
    static final String TYPE_TRIGGER = "Trigger";
    
    /** The TYPE_SEQUENCES. */
    static final String TYPE_SEQUENCES = "Sequences";
    
    /** The TYPE_SEQUENCE. */
    static final String TYPE_SEQUENCE = "Sequence";
    
    /** The TYPE_USER_TYPES. */
    static final String TYPE_USER_TYPES = "User Defined Types";
    
    /** The TYPE_USER_TYPE. */
    static final String TYPE_USER_TYPE = "User Defined Type";
    
    /** The TYPE_DB_LINKS. */
    static final String TYPE_DB_LINKS = "Database Links";
    
    /** The TYPE_DB_LINK. */
    static final String TYPE_DB_LINK = "Database Link";
    
    /** The TYPE_DIRECTORIES. */
    static final String TYPE_DIRECTORIES = "Directories";
    
    /** The TYPE_DIRECTORY. */
    static final String TYPE_DIRECTORY = "Directory";
    
    /** The TYPE_QUEUES. */
    static final String TYPE_QUEUES = "Queues";
    
    /** The TYPE_QUEUE. */
    static final String TYPE_QUEUE = "Queue";
    
    /** The TYPE_QUEUE_TABLES. */
    static final String TYPE_QUEUE_TABLES = "Queue Tables";
    
    /** The TYPE_QUEUE_TABLE. */
    static final String TYPE_QUEUE_TABLE = "Queue Table";
    
    /** The TYPE_XML_SCHEMAS. */
    static final String TYPE_XML_SCHEMAS = "Xml Schemas";
    
    /** The TYPE_XML_SCHEMA. */
    static final String TYPE_XML_SCHEMA = "Xml Schema";
    
    /** The TYPE_FUNCT_PARAMETERS. */
    static final String TYPE_FUNCT_PARAMETERS = "Function parameters";
    
    /** The TYPE_PROC_PARAMETERS. */
    static final String TYPE_PROC_PARAMETERS = "Procedure parameters";
    
    /** The TYPE_PARAMETERS. */
    static final String TYPE_PARAMETERS = "Parameters";
    
    /** The TYPE_PARAMETER. */
    static final String TYPE_PARAMETER = "Parameter";
    
    /** The TYPE_TABLES. */
    static final String TYPE_TABLES = "TABLE";
    
    /** The TYPE_WORKSHEETS. */
    static final String TYPE_WORKSHEETS = "Worksheets";
    
    /** The TYPE_DATA_SET. */
    static final String TYPE_DATA_SET = "Data Set";
    
    /** The TYPE_VIEWS. */
    static final String TYPE_VIEWS = "VIEW";
    
    /** The TYPE_SYSTEM_TABLES. */
    static final String TYPE_SYSTEM_TABLES = "SYSTEM TABLE";
    
    /** The TYPE_SYSTEM_VIEWS. */
    static final String TYPE_SYSTEM_VIEWS = "SYSTEM VIEW";
    
    /** The TYPE_GLOBAL_TEMPORARIES. */
    static final String TYPE_GLOBAL_TEMPORARIES = "GLOBAL TEMPORARY";
    
    /** The TYPE_LOCAL_TEMPORARIES. */
    static final String TYPE_LOCAL_TEMPORARIES = "LOCAL TEMPORARY";
    
    /** The TYPE_TEMPORARY_TABLES. */
    static final String TYPE_TEMPORARY_TABLES = "TEMPORARY TABLE";
    
    /** The TYPE_TEMPORARY_VIEWS. */
    static final String TYPE_TEMPORARY_VIEWS = "TEMPORARY VIEW";
    
    /** The TYPE_ALIASES. */
    static final String TYPE_ALIASES = "ALIAS";
    
    /** The TYPE_SYNONYMS. */
    static final String TYPE_SYNONYMS = "SYNONYM";
    
    /** The TYPE_NICKNAME. */
    static final String TYPE_NICKNAME = "NICKNAME";
    
    /** The TYPE_COLUMNS. */
    static final String TYPE_COLUMNS = "Columns";
    
    /** The TYPE_INDEXES. */
    static final String TYPE_INDEXES = "Indexes";
    
    /** The TYPE_PKS. */
    static final String TYPE_PKS = "Primary Key";
    
    /** The TYPE_FKS. */
    static final String TYPE_FKS = "Foreign Keys";
    
    /** The TYPE_EKS. */
    static final String TYPE_EKS = "Referenced From";
    
    /** The TYPE_TYPE. */
    static final String TYPE_TYPE = "Type";
    
    /** The TYPE_TABLE. */
    static final String TYPE_TABLE = "Table";
    
    /** The TYPE_VIEW. */
    static final String TYPE_VIEW = "View";
    
    /** The TYPE_ALIAS. */
    static final String TYPE_ALIAS = "Alias";
    
    /** The TYPE_SYNONYM. */
    static final String TYPE_SYNONYM = "Synonym";
    
    /** The TYPE_COLUMN. */
    static final String TYPE_COLUMN = "Column";
    
    /** The TYPE_INDEX. */
    static final String TYPE_INDEX = "Index";
    
    /** The TYPE_PK. */
    static final String TYPE_PK = "Primary Key";
    
    /** The TYPE_FK. */
    static final String TYPE_FK = "Foreign Key";
    
    /** The TYPE_EK. */
    static final String TYPE_EK = "External Key";
    
    /** The TYPE_INDEX_COLUMNS. */
    static final String TYPE_INDEX_COLUMNS = "Index Columns";
    
    /** The TYPE_PK_COLUMNS. */
    static final String TYPE_PK_COLUMNS = "Primary Key Columns";
    
    /** The TYPE_FK_COLUMNS. */
    static final String TYPE_FK_COLUMNS = "Foreign Key Columns";
    
    /** The TYPE_EK_COLUMNS. */
    static final String TYPE_EK_COLUMNS = "External Key Columns";
    
    /** The TYPE_INDEX_COLUMN. */
    static final String TYPE_INDEX_COLUMN = "Index Column";
    
    /** The TYPE_PK_COLUMN. */
    static final String TYPE_PK_COLUMN = "Primary Key Column";
    
    /** The TYPE_FK_COLUMN. */
    static final String TYPE_FK_COLUMN = "Foreign Key Column";
    
    /** The TYPE_EK_COLUMN. */
    static final String TYPE_EK_COLUMN = "External Key Column";
    
    /** The METADATA_DELIMITER. */
    static final String METADATA_DELIMITER = "#";
    
    /** The DB_DELIMITER. */
    static final String DB_DELIMITER = ".";
    
    /** The TABLES_DATASET_TYPE. */
    static final String TABLES_DATASET_TYPE = "tables";
    
    /** The DEFAULT_METHOD. */
    static final String DEFAULT_METHOD = "getTablesByType";
    
    /** The DEFAULT_TYPE. */
    static final String DEFAULT_TYPE = TYPE_TABLES;
    
    /**
     * Shows object DDL.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the string
     * @throws Exception
     *             in case of any error
     */
    String asText(String catalog, String schema, String pattern, String type)
        throws Exception;
    
    /**
     * Discovers database types.
     * 
     * @return the map of database types
     * @throws Exception
     *             in case of any error
     */
    Map<Integer, List<FieldDef>> discoverDatabaseTypes()
        throws Exception;
    
    /**
     * Free the resources.
     */
    void free();
    
    /**
     * Gets the connection params provider.
     * 
     * @return the connection params provider
     */
    ConnectionParamsProvider<Alias> getConnectionParamsProvider();
    
    /**
     * Gets the driver.
     * 
     * @return the driver
     */
    Driver getDriver();
    
    /**
     * Gets the full object name.
     * 
     * @param pattern
     *            the pattern
     * @return the full object name
     */
    String getFullObjectName(String pattern);
    
    /**
     * Gets the metadata by type.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the metadata by type
     * @throws Exception
     *             in case of any error
     */
    DataSet getMetadataByType(String catalog, String schema, String pattern,
            String type)
        throws Exception;
    
    /**
     * Gets the metadata type by parent type.
     * 
     * @param parentType
     *            the parent type
     * @return the metadata type by parent type
     */
    String getMetadataTypeByParentType(String parentType);
    
    /**
     * Gets the metadata types.
     * 
     * @param parentType
     *            the parent type
     * @return the metadata types
     * @throws Exception
     *             in case of any error
     */
    List<Object> getMetadataTypes(String parentType)
        throws Exception;
    
    /**
     * Gets the name.
     * 
     * @return the name
     */
    String getName();
    
    /**
     * Gets the object name.
     * 
     * @param pattern
     *            the pattern
     * @return the object name
     */
    String getObjectName(String pattern);
    
    /**
     * Gets the object owner name.
     * 
     * @param pattern
     *            the pattern
     * @return the object owner name
     */
    String getObjectOwnerName(String pattern);
    
    /**
     * Gets the top level database objects. Such as schemas etc.
     * 
     * @return the top level database objects
     * @throws Exception
     *             in case of any error
     */
    List<Object> getTopLevelDbObjects()
        throws Exception;
    
    /**
     * Gets the type methods.
     * 
     * @return the type methods
     */
    Map<String, String> getTypeMethods();
    
    /**
     * Checks if database has catalogs.
     * 
     * @return true, if successful
     */
    boolean hasCatalogs();
    
    /**
     * Checks if database has given metadata type.
     * 
     * @param type
     *            the type
     * @return true, if successful
     */
    boolean hasMetadataTypes(String type);
    
    /**
     * Checks if database has schemas.
     * 
     * @return true, if successful
     */
    boolean hasSchemas();
    
    /**
     * Initializes metadata driver.
     * 
     * @param connectionFactory
     *            the connection factory
     * @param connectionParamsProvider
     *            the connection params provider
     * @param driver
     *            the driver
     * @throws Exception
     *             in case of any error
     */
    void init(ConnectionFactory connectionFactory,
            ConnectionParamsProvider<Alias> connectionParamsProvider,
            Driver driver)
        throws Exception;
    
    /**
     * Checks if database is current. For example if user logs into into the
     * Oracle database using user name "test" the current schema is "test".
     * 
     * @param compareTo
     *            the compare to
     * @return true, if database is current
     */
    boolean isDatabaseCurrent(String compareTo);
    
    /**
     * Checks if metadata driver supports asText method for the type.
     * 
     * @param type
     *            the type
     * @return true, if successful
     */
    boolean supportsAsText(String type);
}