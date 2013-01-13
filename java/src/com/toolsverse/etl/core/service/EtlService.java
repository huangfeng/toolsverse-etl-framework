/*
 * EtlService.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.service;

import com.toolsverse.etl.core.engine.EtlRequest;
import com.toolsverse.etl.core.engine.EtlResponse;
import com.toolsverse.service.Service;

/**
 * <code>EtlService</code> provides support for executing etl scenarios.  
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface EtlService extends Service
{
    
    /**
     * Executes etl scenario using given etl request.
     *
     * @param request the etl request
     * @return the etl response
     * @throws Exception in case of any error
     */
    EtlResponse executeEtl(EtlRequest request)
        throws Exception;
    
}
