/*
 * EtllResource.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.resource;

import com.toolsverse.config.SystemConfig;

/**
 * Messages and errors used by ETL framework.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public enum EtlResource
{
    // errors
    ERROR_RELEASING_CONNECTION("Error releasing connection."),
    ERROR_COMMITING_CONNECTION("Error commiting connection."),
    ERROR_SETTING_CONNECTION_PROPERTIES("Error setting connection properties."),
    ERROR_CLOSING_CONNECTION("Error closing connection."),
    ERROR_POPULATING_METADATA("Error populating metadata."),
    ERROR_CLOSING_RESULT_SET("Error closing ResultSet."),
    ERROR_CLOSING_PREPARED_STATEMENT("Error closing PreparedStatement."),
    ERROR_EXECUTING_TASK("Error executing task."),
    ERROR_JOINING_DATA_SET_WRONG_KEYS_IN_DRIVING("Error joining data sets: wrong key fields in the driving data set."),
    ERROR_JOINING_DATA_SET_WRONG_KEYS_IN_JOIN("Error joining data sets: wrong key fields in the data set to join."),
    ERROR_JOINING_DATA_SET_NO_FIELDS("Error joining data sets: no fields in the result data set."),
    ERROR_UNION_DIFF_FIELDS("Error performing union: data sets have different sets of fields."),
    ERROR_UNION_NO_FIELDS("Error performing union: no fields in the result data set."),
    ERROR_UNION_NO_KEY_FIELDS("Error performing union: no key fields."),
    ERROR_MINUS_DATA_SET_WRONG_KEYS_IN_DRIVING("Error performing minus operation: wrong key fields in the driving data set."),
    ERROR_MINUS_DATA_SET_WRONG_KEYS_IN_MINUS("Error performing minus operation: wrong key fields in the data set to 'minus'."),
    
    // messages
    START_MSG("Start..."),
    FINISH_SUCCESS_MSG("ETL process finished successfully."),
    FINISH_ERROR_MSG("ETL process finished with errors. See log for details."),
    FINISH_NO_REQUEST_ERROR_MSG("ETL process finished. No request passed."),
    FINISH_CONFIG_NOT_INITIALIZED_ERROR_MSG("ETL process finished. Config not initialized."),
    EXECUTION_ERROR_MSG("Error executing ETL process."),
    START_TIME_MSG("Start time: "),
    END_TIME_MSG("End time: "),
    CONFIG_FILE_IS_MISSING_MSG("Configuration file is missing."),
    CONFIG_IS_MISSING_MSG("Config node is missing."),
    CONNECTIONS_IS_MISSING_MSG("Connections node is missing."),
    CONNECTIONS_ARE_MISSING_MSG("There are no connections defined."),
    ACTIVE_CONNECTIONS_ARE_MISSING_MSG("There are no active connections defined."),
    SOURSES_ARE_MISSING_MSG("There are no sourses defined."),
    SOURSE_ALIAS_IS_MISSING_MSG("Sourse Alias is missing."),
    ALIAS_NOT_DEFINED_MSG("Alias is not defined."),
    DESTINATION_IS_MISSING_MSG("Destination is not defined."),
    DESTINATION_ALIAS_IS_MISSING_MSG("Destination Alias is missing."),
    POPULATING_DATASET_MSG("Populating Data Set "),
    PERSISTING_DATASET_MSG("Persisting Data Set "),
    LOADING_BLOB_MSG("Loading blobs from "),
    READING_BLOBS_MSG(" blobs populated"),
    EXECUTING_PRE_TASK("Executing pre task "),
    EXECUTING_INLINE_TASK("Executing inline task "),
    EXECUTING_BEFORE_ETL_TASK("Executing before-etl task "),
    EXECUTING_POST_TASK("Executing post task "),
    LOADING_DATASET_MSG("Loading Data Set "),
    READING_DATASET_MSG(" records populated."),
    PERSITING_RECORD("Persisting record "),
    EXTRACTING_MSG("Extracting source "),
    ADDING_THREAD_FOR_EXTRACTING_MSG("Adding thread for the source "),
    EXTRACTING_ALL_MSG("Extracting..."),
    LOADING_ALL_MSG("Loading..."),
    ADDING_THREAD_FOR_LOADING_MSG("Adding thread for the destination "),
    ADDING_THREAD_FOR_UNIT_MSG("Adding thread for the unit "),
    ADDING_THREAD_FOR_SCENARIO_MSG("Adding thread for the scenario "),
    CREATING_TEMP_TABLES_MSG("Creating temporary tables..."),
    PREPARING_SQL_SCRIPT_MSG("Preparing SQL scripts..."),
    EXCEPTION_MSG("Exception during sql execution: "),
    COMMIT_MSG("Ready to commit..."),
    ERROR_GETTING_METADATA("Error getting metadata:"),
    EXECUTION_SQL_SCRIPT_MSG("Executing script "),
    COMPILE_SQL_SCRIPT_MSG("Compiling script "),
    DELETE_SQL_SCRIPT_MSG("Deleting script "),
    EXECUTION_SQL_SUB_SCRIPT_MSG("Executing sub script for the scenario "),
    PREPARING_SQL_FOR_DESTINATION_MSG("Preparing SQL script for the destination "),
    ASSEMPBLING_SCRIPT_MSG("Assembling script..."),
    MERGE_EXCEPTION_MSG("Exception during merge "),
    DEST_LINE_MSG("Destination "),
    LOG_FILE_DOESNT_EXIST_STR(" log file doesn't exist or syntax error."),
    PLEASE_CHECK_BAD_FILE("Please check "),
    DAT_FILE_DOESNT_EXIST_STR(" dat file doesn't exist."),
    IS_EMPTY_STR(" is empty"),
    EXECUTING_INIT_SCRIPT_MSG("Executing init script... "),
    DATASET_MSG("Data Set "),
    CHECK_CONDITION_MSG("Conditions for the %1 are not satisfied. Block will not be executed."),
    PREPARING_COUNTER_MSG("Preparing scenarion %1 for execution in the loop."),
    EXECUTING_COUNTER_MSG("Executing scenarion %1 in the loop. Loop counter = %2"),
    
    // tasks errors
    CANNOT_CLOSE_CONNECTION("Cannot close connection for the resource "),
    UNKNOWN_COMMAND("Unknown command: "),
    SOURCE_FOLDER_NOT_DEFINED("Source folder is not specified."),
    ZIPFILE_NOT_DEFINED("Zip file name is not specified."),
    ERROR_TRANSOFRMING_FIELD("Error transforming field %1."),
    FILE_COUNT_LESS_THAN_EXPECTED("Number of files is less than expected."),
    
    // dataset parsing errors
    DATA_SET_NAME_IS_MISSING_MSG("Data Set name is missing."),
    DATA_SET_NAME_IS_WRONG_MSG("Data Set name is wrong."),
    FIELD_IS_MISSING_MSG("Field is missing."),
    
    // scenario parsing errors
    SCENARIO_PARSING_ERROR_MSG("Error parsing scenario."),
    SCENARIO_FILE_NAME_IS_MISSING_MSG("Scenario file name is missing."),
    SCENARIO_FILE_IS_MISSING_MSG("Cannot find Scenario File: "),
    ROOT_IS_MISSING_MSG("Root node is missing."),
    SCENARIO_NAME_IS_MISSING_MSG("Scenario name is missing."),
    SCRIPT_NAME_IS_MISSING_MSG("Script name is missing."),
    SOURCES_ARE_MISSING_MSG("Sources are missing."),
    SOURCE_NAME_IS_MISSING_MSG("Source name is missing."),
    KEY_FIELD_NODE_MISSING_MSG("Key Field node is missing."),
    DESTINATIONS_ARE_MISSING_MSG("Destinations are missing."),
    DESTINATION_NAME_IS_MISSING_MSG("Destination name is missing."),
    TASK_NAME_IS_MISSING_MSG("Task name is missing."),
    TASK_CLASS_NAME_IS_MISSING_MSG("Task class name name is missing."),
    ERROR_INNER_NOT_EXIST("Inner scenario \"%1\" does not exist."),
    
    // metadata errors
    ERROR_READING_CATALOGS_MSG("Error reading database catalogs.");
    
    private String _value;
    
    EtlResource(String value)
    {
        _value = value;
    }
    
    /**
     * Gets the value of this enum constant.
     * 
     * @return the value
     */
    public String getValue()
    {
        return SystemConfig.instance().getObjectProperty(this);
    }
    
    @Override
    public String toString()
    {
        return _value;
    }
}
