/*
 * SecurityCredentials.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import com.toolsverse.mvc.model.Model;

/**
 * SecurityCredentials contains security related attributes needed to uniquely identify user. The typical implementation includes user name and password.
 *
 * @see DefaultSecurityCredentials
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public interface SecurityCredentials extends Model
{
    
    /**
     * Checks integrity of the security credentials. For example: there must be a password and it is double checked. 
     *
     * @return null if credentials checked out, otherwise - error string.
     */
    String ckecksOut();
    
    /**
     * Gets the unique id.
     *
     * @return the unique id
     */
    String getId();
}
