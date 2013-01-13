/*
 * SymmetricEncryptorTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.encryption;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.security.GeneralSecurityException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.Utils;

/**
 * SymmetricEncryptorTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SymmetricEncryptorTest
{
    private static final String TEST_STRING = "some test string to encrypt";
    private static final String TEST_KEY_STRING = "sometestkey";
    
    @BeforeClass
    public static void setUp()
    {
        System.setProperty(
                SystemConfig.HOME_PATH_PROPERTY,
                SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue());
        
        SystemConfig.instance().setSystemProperty(
                SystemConfig.DEPLOYMENT_PROPERTY, SystemConfig.TEST_DEPLOYMENT);
        
        Utils.callAnyMethod(SystemConfig.instance(), "init");
    }
    
    @Test(expected = GeneralSecurityException.class)
    public void testBadSecretKey()
        throws Exception
    {
        SymmetricEncryptor.getSecretKey("");
    }
    
    @Test
    public void testDecrypt()
        throws GeneralSecurityException
    {
        String newString = SymmetricEncryptor.decrypt(SymmetricEncryptor
                .getSecretKey(TEST_KEY_STRING), SymmetricEncryptor.encrypt(
                SymmetricEncryptor.getSecretKey(TEST_KEY_STRING), TEST_STRING));
        
        assertTrue(TEST_STRING.equals(newString));
    }
    
    @Test
    public void testDecryptUsingDefaultKey()
        throws GeneralSecurityException
    {
        String newString = SymmetricEncryptor.decrypt(SymmetricEncryptor
                .encrypt(TEST_STRING));
        
        assertTrue(TEST_STRING.equals(newString));
    }
    
    public void testEncrypt()
        throws GeneralSecurityException
    {
        String encryptedString = SymmetricEncryptor.encrypt(
                SymmetricEncryptor.getSecretKey(TEST_KEY_STRING), TEST_STRING);
        
        assertTrue(!TEST_STRING.equals(encryptedString));
    }
    
    @Test
    public void testEncryptUsingDefaultKey()
        throws GeneralSecurityException
    {
        String encryptedString = SymmetricEncryptor.encrypt(TEST_STRING);
        
        assertTrue(!TEST_STRING.equals(encryptedString));
    }
    
    @Test
    public void testGetSecretKey()
        throws GeneralSecurityException
    {
        assertNotNull(SymmetricEncryptor.getSecretKey(TEST_KEY_STRING));
    }
}
