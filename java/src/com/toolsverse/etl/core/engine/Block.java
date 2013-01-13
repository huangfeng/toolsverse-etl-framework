/*
 * Block.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.sql.Connection;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.util.ListHashMap;

/**
 * This interface is a main building block of the etl scenario.
 * The actual implementations are <code>Source</code> and
 * <code>Destination</code> classes.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface Block
{
    
    /**
     * Gets the tasks which are executed before etl framework starts processing
     * the <code>DataSet</code> object associated with the particular scenario
     * block but after initializing is done. For example: after metadata are
     * collected.
     * 
     * @return the tasks as an <code>ListHashMap</code> object
     */
    ListHashMap<String, Task> getBeforeEtlTasks();
    
    /**
     * Gets the actual JDBC connection associated with the scenario block.
     * 
     * @return the connection
     */
    Connection getConnection();
    
    /**
     * Gets the name of the connection associated with the scenario block. By
     * default (if not named specifically) the name of the source connection is
     * a "source" and the name of the destination connection is "dest". Connection
     * can be referenced by name from the scenario block such as
     * <code>Source</code> or <code>Destination</code>
     * 
     * <p>
     * <b>Example</b> of named connection in the scenario file:</br>
     * <blockquote>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>image</name>}</dt> </br>
     * <dd>{@code <source>image</source>}</dt> </br>
     * <dd>{@code <load>}</dt> </br>
     * <dd>{@code <connection>mssql</connection>}</dt> </br>
     * <dd>{@code <driver>com.toolsverse.etl.driver.mysql.MsSqlDriver</driver>}</dt>
     * </br>
     * <dd>{@code </load>}</dt> </br>
     * <dt>{@code </destination>}</dt> </blockquote>
     * 
     * @return the name of the connection
     * 
     */
    String getConnectionName();
    
    /**
     * Gets the <code>DataSet</code> object associated with the scenario block.
     * 
     * @return the data set
     */
    DataSet getDataSet();
    
    /**
     * Gets the driver class name.
     *
     * @return the driver class name
     */
    String getDriverClassName();
    
    /**
     * Gets the tasks which are executed for each row of the
     * <code>DataSet</code> object associated with the scenario block.
     * 
     * @return the inline tasks as an <code>ListHashMap</code> object
     */
    ListHashMap<String, Task> getInlineTasks();
    
    /**
     * Gets name of the block.
     *
     * @return the name name of the block
     */
    String getName();
    
    /**
     * Gets the tasks which are executed after etl framework finished with the
     * processing of the particular scenario block.
     * 
     * @return the post tasks as an <code>ListHashMap</code> object
     */
    ListHashMap<String, Task> getPostTasks();
    
    /**
     * Gets the tasks which are executed before etl framework starts processing
     * of the particular scenario block.
     * 
     * @return the tasks as an <code>ListHashMap</code> object
     */
    ListHashMap<String, Task> getTasks();
    
    /**
     * Gets the variable by name.
     *
     * @param name the name of the variable
     * @return the variable
     */
    Variable getVariable(String name);
    
    /**
     * Gets the variables.
     * 
     * @return the variables
     */
    ListHashMap<String, Variable> getVariables();
    
    /**
     * Checks if <code>DataSet</code> object associated with the scenario block
     * is empty. Scenario block can be configured to be treated as empty
     * regardless of the actual state of the <code>DataSet</code> object.
     * 
     * @return true, if empty
     */
    boolean isEmpty();
    
    /**
     * Checks if scenario block is enabled. For disabled blocks no code will be
     * executed.
     * 
     * @return true, if enabled
     */
    boolean isEnabled();
    
    /**
     * Checks if encoded flag set for the <code>DataSet</code> object associated
     * with the scenario block. Usually inly Clob and Blob fields can be encoded. ETL
     * framework uses open source implementation of the Base64 encoding.
     * 
     * @return true, if encoded
     */
    boolean isEncoded();
    
    /**
     * Checks if code for the particular scenario block can be executed in the
     * separate thread.
     * 
     * @return true, if is parallel
     */
    boolean isParallel();
    
    /**
     * Check if no database connection should be assosiated with the block.
     *
     * @return true, if successful
     */
    boolean noConnection();
    
    /**
     * Sets the tasks which are executed before etl framework starts processing
     * the <code>DataSet</code> object associated with the particular scenario
     * block but after initializing is done. For example after metadata are
     * collected. Tasks get instantiated by parsing scenario file.
     * 
     * @param value
     *            the new before etl tasks
     */
    void setBeforeEtlTasks(ListHashMap<String, Task> value);
    
    /**
     * Sets the actual JDBC connection associated with the scenario block.
     * 
     * @param value
     *            the new connection
     */
    void setConnection(Connection value);
    
    /**
     * Sets the name of the connection associated with the scenario block.
     * 
     * @param value
     *            the new name of the connection
     */
    void setConnectionName(String value);
    
    /**
     * Sets the <code>DataSet</code> object associated with the scenario block.
     * 
     * @param value
     *            the new data set
     */
    void setDataSet(DataSet value);
    
    /**
     * Sets the driver class name.
     *
     * @param value the new driver class name
     */
    void setDriverClassName(String value);
    
    /**
     * Sets the enabled property of the scenario block. The default value is
     * <code>true</code>.
     * 
     * <p>
     * <b>Example</b> of the manually set enabled flag in the scenario
     * file:</br> <blockquote>
     * <dt>{@code <source enabled="No">}</dt>
     * </br>
     * <dd>{@code <name>test</name>}</dd>
     * </br>
     * <dd>{@code <extract>}</dd>
     * </br>
     * <dd>{@code <sql>select * from test</sql>}</dd>
     * </br>
     * <dd>{@code </extract>}</dd>
     * </br>
     * <dt>{@code </source>}</dt> </br> </blockquote>
     * 
     * @param value
     *            the new enabled
     */
    void setEnabled(boolean value);
    
    /**
     * Sets the encoded flag for the <code>DataSet</code> object associated with
     * the scenario block. By default all <code>DataSet</code> objects which
     * contain Clob or Blob fields are encoded and compressed using zip
     * algorithm.
     * 
     * <p>
     * <b>Example</b> of the manually set encoded flag in the scenario
     * file:</br> <blockquote>
     * <dt>{@code <source encode="No">}</dt>
     * </br>
     * <dd>{@code <name>clobs</name>}</dd>
     * </br>
     * <dd>{@code <extract>}</dd>
     * </br>
     * <dd>{@code <sql>select * from clobs</sql>}</dd>
     * </br>
     * <dd>{@code </extract>}</dd>
     * </br>
     * <dt>{@code </source>}</dt> </br> </blockquote>
     * 
     * @param value
     *            the new encoded
     */
    void setEncoded(boolean value);
    
    /**
     * Sets the tasks which are executed for each row of the
     * <code>DataSet</code> object associated with the scenario block. Inline
     * tasks get instantiated by parsing scenario file.
     * <p>
     * <b>Example</b> of the inline task in the scenario file:</br> <blockquote>
     * <dt>{@code <tasks>}</dt>
     * </br>
     * <dd>{@code <task>}</dd>
     * </br>
     * <dd>{@code <name>replace</name>}</dd>
     * </br>
     * <dd>
     * {@code <class>com.toolsverse.etl.core.task.common.RegexpTransformator</class>}
     * </dd>
     * </br>
     * <dd>{@code <variables>}</dd>
     * </br>
     * <dd>{@code <FIELD value="DESCRIPTION" />}</dd>
     * </br>
     * <dd>{@code <REGEXP value="Substitution" />}</dd>
     * </br>
     * <dd>{@code <REPLACE value="something" />}</dd>
     * </br>
     * <dd>{@code </variables>}</dd>
     * </br>
     * <dd>{@code </task>}</dd>
     * </br>
     * <dt>{@code </tasks>}</dt> </br> </blockquote>
     */
    void setInlineTasks(ListHashMap<String, Task> value);
    
    /**
     * Sets the state of the field <code>isEmpty</code> for the scenario block
     * regardless of the actual state of the <code>DataSet</code> object. In the
     * majority of cases etl framework can automatically set this flag however
     * it doesn't hurt to do it manually.
     * 
     * <p>
     * <b>Example</b> of the always empty scenario block in the scenario
     * file:</br> <blockquote>
     * <dt>{@code <source empty="Yes">}</dt>
     * </br>
     * <dd>{@code <name>init</name>}</dd>
     * </br>
     * <dd>{@code <extract>}</dd>
     * </br>
     * <dd>{@code <tasks>}</dd>
     * </br>
     * <dd>{@code <task>}</dd>
     * </br>
     * <dd>{@code <name>prepare</name>}</dd>
     * </br>
     * <dd>{@code <class>com.toolsverse.etl.core.task.sql.SqlTask</class>}</dd>
     * </br>
     * <dd>{@code <sql>drop table tmp_cp</sql>}</dd>
     * </br>
     * <dd>{@code <onexception action="ignore"/>}</dd>
     * </br>
     * <dd>{@code </task>}</dd>
     * </br>
     * <dd>{@code </tasks>}</dd>
     * </br>
     * <dd>{@code </extract>}</dd>
     * </br>
     * <dt>{@code </source>}</dt> </br> </blockquote>
     * 
     * @param value
     *            the new isEmpty
     */
    void setIsEmpty(boolean value);
    
    /**
     * Sets the name of the block.
     * 
     * <p>
     * <b>Example</b> of the setting of the Destination name in scenario
     * file:</br> <blockquote>
     * <dt>{@code <destination>}</dt>
     * <dd>{@code <name>image</name>}</dt>
     * <dt>{@code </destination>}</dt>
     * </blockquote>
     * 
     * <p>
     * <b>Example</b> of the setting of the Source name in scenario file:</br>
     * <blockquote>
     * <dt>{@code <source>}</dt>
     * <dd>{@code <name>image</name>}</dt>
     * <dt>{@code </source>}</dt> </blockquote>
     * 
     * @param value
     *            The new name
     */
    void setName(String value);
    
    /**
     * Sets the the property isParallel for the scenario block. If set to
     * <code>true</code> the code for this block can be executed in the separate
     * thread. The default value is <code>false</code>.
     * 
     * <p>
     * <b>Example</b> of the parallel scenario block in the scenario file:</br>
     * <blockquote>
     * <dt>{@code <source parallel="Yes">}</dt>
     * </br>
     * <dd>{@code <name>test</name>}</dd>
     * </br>
     * <dd>{@code <extract>}</dd>
     * </br>
     * <dd>{@code <sql>select * from test</sql>}</dd>
     * </br>
     * <dd>{@code </extract>}</dd>
     * </br>
     * <dt>{@code </source>}</dt> </br> </blockquote>
     * 
     * @param value
     *            the new parallel
     */
    void setParallel(boolean value);
    
    /**
     * Sets the tasks which are executed after etl framework finished with the
     * processing of the particular scenario block. Post tasks get instantiated
     * by parsing scenario file.
     * <p>
     * <b>Example</b> of the post task in the scenario file:</br> <blockquote>
     * <dt>{@code <tasks>}</dt>
     * </br>
     * <dd>{@code <task scope="after">}</dd>
     * </br>
     * <dd>{@code <name>delete</name>}</dd>
     * </br>
     * <dd>{@code <class>com.toolsverse.etl.core.task.common.FileManagerTask</class>}</dd>
     * </br>
     * <dd>{@code <variables>}</dd>
     * </br>
     * <dd>{@code <COMMAND value="delete" />}</dd>
     * </br>
     * <dd>{@code <SOURCE_TYPE value="file" />}</dd>
     * </br>
     * <dd>{@code <FILES value="*.dat" />}</dd>
     * </br>
     * <dd>{@code </variables>}</dd>
     * </br>
     * <dd>{@code </task>}</dd>
     * </br>
     * <dt>{@code </tasks>}</dt> </br> </blockquote>
     * 
     * @param value
     *            the new post tasks
     */
    void setPostTasks(ListHashMap<String, Task> value);
    
    /**
     * Sets the tasks which are executed before etl framework starts processing
     * of the particular scenario block. Tasks get instantiated by parsing
     * scenario file.
     * <p>
     * <b>Example</b> of the task in the scenario file:</br> <blockquote>
     * <dt>{@code <tasks>}</dt>
     * </br>
     * <dd>{@code <task>}</dd>
     * </br>
     * <dd>{@code <name>copy</name>}</dd>
     * </br>
     * <dd>{@code <connection>dest</connection>}</dd>
     * </br>
     * <dd>{@code <class>com.toolsverse.etl.core.task.oracle.OracleCopyTask</class>}</dd>
     * </br>
     * <dd>{@code <sql>}</dd>
     * </br>
     * <dd>{@code insert temp USING}</dd>
     * </br>
     * <dd>{@code select * from some_table}</dd>
     * </br>
     * <dd>{@code </sql>}</dd>
     * </br>
     * <dd>{@code </task>}</dd>
     * </br>
     * <dt>{@code </tasks>}</dt> </br> </blockquote>
     * 
     * @param value
     *            the new tasks
     */
    void setTasks(ListHashMap<String, Task> value);
    
    /**
     * Sets the variables for the block. Variables get instantiated by parsing
     * scenario file.
     * <p>
     * <b>Example</b> of the variables in the scenario file:</br> <blockquote>
     * <dt>{@code <variables>}</dt>
     * </br>
     * <dd>{@code <STEP_NUM function="getPk" />}</dd>
     * </br>
     * <dd>{@code <CONFIG_NUM function="getFk" />}</dd>
     * </br>
     * <dd>{@code <USER function="getUser" />}</dd>
     * </br>
     * <dd>{@code <ID include="No" />}</dd>
     * </br>
     * <dt>{@code </variables>}</dt> </br> </blockquote>
     * 
     * @param value
     *            The new variables
     */
    void setVariables(ListHashMap<String, Variable> value);
}
