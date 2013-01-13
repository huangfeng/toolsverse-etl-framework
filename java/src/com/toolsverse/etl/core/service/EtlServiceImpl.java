/*
 * EtlServiceImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.service;

import com.toolsverse.etl.core.engine.EtlProcess;
import com.toolsverse.etl.core.engine.EtlRequest;
import com.toolsverse.etl.core.engine.EtlResponse;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.util.log.Logger;

/**
 * The default implementation of the <code>EtlService</code> interface.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlServiceImpl implements EtlService
{
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.service.EtlService#executeEtl(com.toolsverse.
     * etl.core.engine.EtlRequest)
     */
    public EtlResponse executeEtl(EtlRequest request)
        throws Exception
    {
        try
        {
            Logger.getLogger().setLevel(EtlLogger.class, request.getLogLevel());
            
            EtlProcess etlProcess = new EtlProcess(EtlProcess.EtlMode.INCLUDED);
            
            return etlProcess.execute(request);
        }
        finally
        {
            Logger.getLogger().setLevel(EtlLogger.class, Logger.SEVERE);
        }
    }
    
}
