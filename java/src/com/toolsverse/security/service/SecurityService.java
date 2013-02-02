/*
 * SecurityService.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security.service;

import java.util.Map;

import com.toolsverse.security.Role;
import com.toolsverse.security.SecurityContext;
import com.toolsverse.security.SecurityCredentials;
import com.toolsverse.security.SecurityModel;
import com.toolsverse.service.Service;

/**
 * The interface for the authorization security service. In the typical scenario getRoles called first then login. The login must check if there are
 * any roles associated with the user which are also belong to the map returned by getRoles. If if there none - the login must fail with the appropriate
 * error message. 
 *
 * @param <C> the generic type which must implement SecurityCredentials
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public interface SecurityService<C extends SecurityCredentials> extends Service
{
    
    /**
     * Gets all the roles defined in the system. Typically called once when security manager is initialized. 
     *
     * @param defaultRoles the default roles, such as Administrator. They are automatically added to the top of the list.
     * @return the roles
     * @throws Exception in case of any error
     */
    Map<String, Role> getRoles(Map<String, String> defaultRoles)
        throws Exception;
    
    /**
     * Login.
     *
     * @param credentials the credentials
     * @return the security model
     * @throws Exception in case of any error
     */
    SecurityModel login(C credentials)
        throws Exception;
    
    /**
     * Logout.
     *
     * @param context the context
     * @return true, if successful
     * @throws Exception in case of any error
     */
    boolean logout(SecurityContext context)
        throws Exception;
}
