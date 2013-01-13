/*
 * XslTransormTask.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.connector.ConnectorResource;
import com.toolsverse.etl.connector.xml.XmlConnectorParams;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.OnTask;
import com.toolsverse.etl.core.engine.Task;
import com.toolsverse.etl.core.engine.TaskResult;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.Utils;

/**
 * This is a {@link Task#PRE} and {@link Task#POST} task which performs xsl transformation on given file. 
 * 
 * @see com.toolsverse.etl.core.engine.Task
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class XslTransormTask implements OnTask
{
    
    /** The SOURCE_FILE_VAR  - the value of this variable defines source file name. */
    public static final String SOURCE_FILE_VAR = "SOURCE";
    
    /** The DESTINATION_FILE_VAR - the value of this variable defines destination file name. */
    public static final String DESTINATION_FILE_VAR = "DESTINATION";
    
    /** The XSL_FILE_VAR - the value of this variable defines xsl file name. */
    public static final String XSL_FILE_VAR = "XSL";
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executeBeforeEtlTask(com.toolsverse
     * .etl.core.config.EtlConfig, com.toolsverse.etl.core.engine.Task)
     */
    public TaskResult executeBeforeEtlTask(EtlConfig config, Task task)
        throws Exception
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executeInlineTask(com.toolsverse
     * .etl.core.config.EtlConfig, com.toolsverse.etl.core.engine.Task, long)
     */
    public TaskResult executeInlineTask(EtlConfig config, Task task, long index)
        throws Exception
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executePostTask(com.toolsverse.
     * etl.core.config.EtlConfig, com.toolsverse.etl.core.engine.Task,
     * com.toolsverse.etl.common.DataSet)
     */
    public TaskResult executePostTask(EtlConfig config, Task task,
            DataSet dataSet)
        throws Exception
    {
        executePreTask(config, task);
        
        return new TaskResult(dataSet);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executePreTask(com.toolsverse.etl
     * .core.config.EtlConfig, com.toolsverse.etl.core.engine.Task)
     */
    public TaskResult executePreTask(EtlConfig config, Task task)
        throws Exception
    {
        String sourceFileName = EtlUtils.getVarValue(task.getVariables(),
                SOURCE_FILE_VAR, null);
        
        String destFileName = EtlUtils.getVarValue(task.getVariables(),
                DESTINATION_FILE_VAR, null);
        
        String xsltFileName = EtlUtils.getVarValue(task.getVariables(),
                XSL_FILE_VAR, null);
        
        if (Utils.isNothing(destFileName))
            throw new Exception(
                    ConnectorResource.TRANSFORMATION_ERROR_NO_DEST.getValue());
        
        // use alias
        if (Utils.isNothing(sourceFileName))
        {
            Alias alias = (Alias)config.getConnectionFactory()
                    .getConnectionParams(task.getConnectionName());
            
            if (alias == null || Utils.isNothing(alias.getUrl()))
                throw new Exception(
                        ConnectorResource.TRANSFORMATION_ERROR_NO_SOURCE
                                .getValue());
            
            sourceFileName = alias.getUrl();
            
            if (Utils.isNothing(xsltFileName))
            {
                Properties props = Utils.getProperties(alias.getParams());
                
                if (props != null)
                    xsltFileName = props
                            .getProperty(XmlConnectorParams.XSL_TO_FILE_NAME_PROP);
            }
        }
        
        sourceFileName = SystemConfig.instance().getPathUsingAppFolders(
                FileUtils.getFilename(sourceFileName, SystemConfig.instance()
                        .getDataFolderName(), "*.xml", false));
        
        xsltFileName = SystemConfig.instance().getPathUsingAppFolders(
                FileUtils.getFilename(xsltFileName, SystemConfig.instance()
                        .getDataFolderName(), "*.xsl", false));
        
        destFileName = SystemConfig.instance().getPathUsingAppFolders(
                FileUtils.getFilename(destFileName, SystemConfig.instance()
                        .getDataFolderName(), "*.xml", false));
        
        transform(sourceFileName, destFileName, xsltFileName);
        
        return new TaskResult();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#init(com.toolsverse.etl.core.config
     * .EtlConfig, com.toolsverse.etl.core.engine.Task)
     */
    public void init(EtlConfig config, Task task)
        throws Exception
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isInlineTask()
     */
    public boolean isInlineTask()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isPostTask()
     */
    public boolean isPostTask()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isPreEtlTask()
     */
    public boolean isPreEtlTask()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isPreTask()
     */
    public boolean isPreTask()
    {
        return true;
    }
    
    /**
     * Transforms given output stream into input stream using xsl style sheet defined by <code>xsltSource</code>.
     *
     * @param out the out
     * @param inputSource the input source
     * @param xsltSource the xslt source
     * @throws Exception in case of any error
     */
    private void transform(OutputStream out, Source inputSource,
            Source xsltSource)
        throws Exception
    {
        try
        {
            Result result = new StreamResult(out);
            
            // create an instance of TransformerFactory
            TransformerFactory transFact = TransformerFactory.newInstance();
            
            Transformer trans = transFact.newTransformer(xsltSource);
            
            trans.transform(inputSource, result);
        }
        finally
        {
            if (out != null)
                out.close();
        }
    }
    
    /**
     * Transforms source file into destination file using xsl style sheet defined by <code>xsltFileName</code>. 
     *
     * @param sourceFileName the source file name
     * @param destFileName the destination file name
     * @param xsltFileName the xslt file name
     * @throws Exception in case of any error
     */
    private void transform(String sourceFileName, String destFileName,
            String xsltFileName)
        throws Exception
    {
        File sourceFile = new File(sourceFileName);
        File xsltFile = new File(xsltFileName);
        
        if (!sourceFile.exists())
            throw new Exception(
                    ConnectorResource.TRANSFORMATION_ERROR_NO_SOURCE.getValue());
        
        if (!xsltFile.exists())
            throw new Exception(
                    ConnectorResource.TRANSFORMATION_ERROR_NO_XSL.getValue());
        
        transform(new FileOutputStream(destFileName), new StreamSource(
                sourceFile), new StreamSource(xsltFile));
    }
    
}
