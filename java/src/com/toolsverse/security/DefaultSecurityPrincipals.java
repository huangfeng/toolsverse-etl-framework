/*
 * DefaultSecurityPrincipals.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import com.toolsverse.mvc.model.Getter;
import com.toolsverse.mvc.model.ModelImpl;
import com.toolsverse.mvc.model.Setter;

/**
 * The default implementation of the SecurityPrincipals. Includes first name, last name and email.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public class DefaultSecurityPrincipals extends ModelImpl implements
        SecurityPrincipals
{
    
    /** The FIRST_NAME. */
    public static final String FIRST_NAME = "principalsfirstname";
    
    /** The LAST_NAME. */
    public static final String LAST_NAME = "principalslastname";
    
    /** The EMAIL. */
    public static final String EMAIL = "principalsemail";
    
    /**
     * Gets the email.
     *
     * @return the email
     */
    @Getter(name = EMAIL)
    public String getEmail()
    {
        return (String)getAttributeValue(EMAIL);
    }
    
    /**
     * Gets the first name.
     *
     * @return the first name
     */
    @Getter(name = FIRST_NAME)
    public String getFirstName()
    {
        return (String)getAttributeValue(FIRST_NAME);
    }
    
    /**
     * Gets the last name.
     *
     * @return the last name
     */
    @Getter(name = LAST_NAME)
    public String getLastName()
    {
        return (String)getAttributeValue(LAST_NAME);
    }
    
    /**
     * Sets the email.
     *
     * @param value the new email
     */
    @Setter(name = EMAIL)
    public void setEmail(String value)
    {
        setAttributeValue(EMAIL, value);
    }
    
    /**
     * Sets the first name.
     *
     * @param value the new first name
     */
    @Setter(name = FIRST_NAME)
    public void setFirstName(String value)
    {
        setAttributeValue(FIRST_NAME, value);
    }
    
    /**
     * Sets the last name.
     *
     * @param value the new last name
     */
    @Setter(name = LAST_NAME)
    public void setLastName(String value)
    {
        setAttributeValue(LAST_NAME, value);
    }
}
