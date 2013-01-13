/*
 * SymmetricEncryptor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.encryption;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

import com.toolsverse.resource.Resource;
import com.toolsverse.security.resource.SecurityResource;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * SymmetricEncryptor uses symmetric key to encrypt and decrypt strings, streams and files.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */
public class SymmetricEncryptor
{
    
    /** The Constant SECRET_KEY. */
    private static final SecretKey SECRET_KEY = getSecretKey();
    
    /**
     * Uses the default key to decrypt an InputStream.
     *
     * @param encryptedStream the InputStream
     * @return the decrypted InputStream
     * @throws GeneralSecurityException the general security exception
     */
    public static InputStream decrypt(InputStream encryptedStream)
        throws GeneralSecurityException
    {
        return decrypt(SECRET_KEY, encryptedStream);
    }
    
    /**
     * Uses the specific key to decrypt an InputStream.
     *
     * @param key the encryption key
     * @param encryptedStream the InputStream
     * @return the decrypted InputStream
     * @throws GeneralSecurityException the general security exception
     */
    public static InputStream decrypt(Key key, InputStream encryptedStream)
        throws GeneralSecurityException
    {
        Cipher dcipher = Cipher.getInstance("DES");
        dcipher.init(Cipher.DECRYPT_MODE, key);
        return new CipherInputStream(encryptedStream, dcipher);
    }
    
    /**
     * Uses the specific key to decrypt string.
     * 
     * @param key the encryption key 
     * @param encryptedStr the encrypted string       
     * @return the decrypted string
     * @throws GeneralSecurityException the general security exception
     */
    public static String decrypt(Key key, String encryptedStr)
        throws GeneralSecurityException
    {
        try
        {
            if (encryptedStr.startsWith("{3Des}"))
            {
                encryptedStr = encryptedStr
                        .substring(encryptedStr.indexOf("}") + 1);
                Cipher dcipher = Cipher.getInstance("DES");
                dcipher.init(Cipher.DECRYPT_MODE, key);
                byte[] nobase64 = new sun.misc.BASE64Decoder()
                        .decodeBuffer(encryptedStr);
                String secret = new String(dcipher.doFinal(nobase64));
                return secret;
            }
            else
                throw new GeneralSecurityException(
                        SecurityResource.ERROR_DECRIPTING.getValue());
            
        }
        catch (GeneralSecurityException e)
        {
            throw e;
        }
        catch (Exception ex)
        {
            throw new GeneralSecurityException(ex);
        }
    }
    
    /**
     * Uses the default key to decrypt string.
     * 
     * @param encryptedStr the encrypted string       
     * @return the decrypted string
     * @throws GeneralSecurityException the general security exception
     */
    public static String decrypt(String encryptedStr)
        throws GeneralSecurityException
    {
        return decrypt(SECRET_KEY, encryptedStr);
    }
    
    /**
     * Uses the specific key to decrypt file.
     * 
     * @param key the encryption key 
     * @param encryptedFile the name of the encrypted file
     * @throws GeneralSecurityException the general security exception
     */
    public static void decryptFile(Key key, String encryptedFile)
        throws GeneralSecurityException
    {
        try
        {
            Cipher dcipher = Cipher.getInstance("DES");
            dcipher.init(Cipher.DECRYPT_MODE, key);
            
            FileInputStream fIn = null;
            FileOutputStream fOut = null;
            
            File workFile = new File(encryptedFile);
            File tempFile = new File(workFile.getPath().substring(0,
                    workFile.getPath().indexOf(workFile.getName()))
                    + Utils.getUUID());
            
            if (!workFile.exists())
                return;
            
            boolean isOk = false;
            
            try
            {
                fIn = new FileInputStream(encryptedFile);
                fOut = new FileOutputStream(tempFile.getPath());
                byte[] buffer = new byte[4096];
                int i;
                while ((i = fIn.read(buffer)) != -1)
                {
                    fOut.write(dcipher.update(buffer, 0, i));
                }
                fOut.write(dcipher.doFinal());
                
                isOk = true;
            }
            catch (Exception ex)
            {
                tempFile.delete();
                throw ex;
            }
            finally
            {
                if (fOut != null)
                    fOut.close();
                if (fIn != null)
                    fIn.close();
                
                if (isOk)
                {
                    workFile.delete();
                    
                    tempFile.renameTo(workFile);
                }
            }
        }
        catch (GeneralSecurityException e)
        {
            throw e;
        }
        catch (Exception ex)
        {
            throw new GeneralSecurityException(ex);
        }
        
    }
    
    /**
     * Uses the default key to decrypt file.
     * 
     * @param encryptedFile the name of the encrypted file
     * @throws GeneralSecurityException the general security exception
     */
    public static void decryptFile(String encryptedFile)
        throws GeneralSecurityException
    {
        decryptFile(SECRET_KEY, encryptedFile);
    }
    
    /**
     * Decrypts password.
     *
     * @param key the key
     * @param encryptedStr the encrypted password
     * @return the decrypted password
     * @throws GeneralSecurityException the general security exception
     */
    public static String decryptPassword(Key key, String encryptedStr)
        throws GeneralSecurityException
    {
        key = key != null ? key : SECRET_KEY;
        
        try
        {
            return decrypt(key, encryptedStr);
        }
        catch (GeneralSecurityException ex)
        {
            if (key != SECRET_KEY)
                return decrypt(SECRET_KEY, encryptedStr);
            else
                throw ex;
        }
        
    }
    
    /**
     * Encrypts the OutputStream using specific key.
     *
     * @param key the encryption key
     * @param streamToEncrypt the OutputStream to encrypt
     * @return the output stream
     * @throws GeneralSecurityException the general security exception
     */
    public static OutputStream encrypt(Key key, OutputStream streamToEncrypt)
        throws GeneralSecurityException
    {
        Cipher dcipher = Cipher.getInstance("DES");
        dcipher.init(Cipher.ENCRYPT_MODE, key);
        return new CipherOutputStream(streamToEncrypt, dcipher);
    }
    
    /**
     * Encrypts string using specific key.
     *
     * @param key the encryption key
     * @param strToEncrypt the string to encrypt
     * @return the string
     * @throws GeneralSecurityException the general security exception
     */
    public static String encrypt(Key key, String strToEncrypt)
        throws GeneralSecurityException
    {
        Cipher c = Cipher.getInstance("DES");
        c.init(Cipher.ENCRYPT_MODE, key);
        byte enc[] = c.doFinal(strToEncrypt.getBytes());
        String base64 = new sun.misc.BASE64Encoder().encode(enc);
        return "{3Des}" + base64;
    }
    
    /**
     * Encrypts the OutputStream using default key.
     *
     * @param streamToEncrypt the OutputStream to encrypt
     * @return the output stream
     * @throws GeneralSecurityException the general security exception
     */
    public static OutputStream encrypt(OutputStream streamToEncrypt)
        throws GeneralSecurityException
    {
        return encrypt(SECRET_KEY, streamToEncrypt);
    }
    
    /**
     * Encrypts string using default key.
     *
     * @param strToEncrypt the string to encrypt
     * @return the string
     * @throws GeneralSecurityException the general security exception
     */
    public static String encrypt(String strToEncrypt)
        throws GeneralSecurityException
    {
        return encrypt(SECRET_KEY, strToEncrypt);
    }
    
    /**
     * Encrypts the file using specific key.
     * 
     * @param key the encryption key 
     * @param fileToEncrypt the name of the file to encrypt
     * @throws GeneralSecurityException the general security exception
     */
    public static void encryptFile(Key key, String fileToEncrypt)
        throws GeneralSecurityException
    {
        try
        {
            Cipher c = Cipher.getInstance("DES");
            c.init(Cipher.ENCRYPT_MODE, key);
            
            FileInputStream fIn = null;
            FileOutputStream fOut = null;
            
            File workFile = new File(fileToEncrypt);
            File tempFile = new File(workFile.getPath().substring(0,
                    workFile.getPath().indexOf(workFile.getName()))
                    + Utils.getUUID());
            
            if (!workFile.exists())
                return;
            
            boolean isOk = false;
            
            try
            {
                fIn = new FileInputStream(fileToEncrypt);
                fOut = new FileOutputStream(tempFile.getPath());
                byte[] buffer = new byte[4096];
                int i;
                while ((i = fIn.read(buffer)) != -1)
                    fOut.write(c.update(buffer, 0, i));
                
                fOut.write(c.doFinal());
                
                isOk = true;
            }
            catch (Exception ex)
            {
                tempFile.delete();
                throw ex;
            }
            finally
            {
                if (fOut != null)
                    fOut.close();
                if (fIn != null)
                    fIn.close();
                
                if (isOk)
                {
                    workFile.delete();
                    
                    tempFile.renameTo(workFile);
                }
            }
        }
        catch (Exception ex)
        {
            throw new GeneralSecurityException(ex);
        }
        
    }
    
    /**
     * Encrypts the file using default key.
     * 
     * @param fileToEncrypt the name of the file to encrypt
     * @throws GeneralSecurityException the general security exception
     */
    public static void encryptFile(String fileToEncrypt)
        throws GeneralSecurityException
    {
        encryptFile(SECRET_KEY, fileToEncrypt);
    }
    
    /**
     * Encrypts password.
     *
     * @param key the key
     * @param strToEncrypt the password to encrypt
     * @return the encrypted password
     * @throws GeneralSecurityException the general security exception
     */
    public static String encryptPassword(Key key, String strToEncrypt)
        throws GeneralSecurityException
    {
        key = key != null ? key : SECRET_KEY;
        
        return encrypt(key, strToEncrypt);
    }
    
    /**
     * Builds the public key from the array of bytes key using given algorithm.
     *
     * @param bytes the bytes
     * @param algorithm the algorithm
     * @return the public key
     * @throws InvalidKeySpecException the invalid key spec exception
     * @throws NoSuchAlgorithmException the no such algorithm exception
     */
    
    public static PublicKey getPublicKey(byte[] bytes, String algorithm)
        throws InvalidKeySpecException, NoSuchAlgorithmException
    {
        KeyFactory keyFactory = KeyFactory.getInstance(algorithm);
        EncodedKeySpec keySpec = new X509EncodedKeySpec(bytes);
        
        PublicKey key = keyFactory.generatePublic(keySpec);
        
        return key;
    }
    
    /**
     * Gets the default key.
     * @return SecretKey
     */
    private static SecretKey getSecretKey()
    {
        try
        {
            return SecretKeyFactory.getInstance("DES").generateSecret(
                    new DESKeySpec(new byte[] {(byte)0x17, (byte)0x31,
                            (byte)0x57, (byte)0x22, (byte)0x7f, (byte)0x9e,
                            (byte)0x51, (byte)0x12}));
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, null, Resource.ERROR_GENERAL.getValue(),
                    ex);
            
            return null;
        }
    }
    
    /**
     * Gets the SecretKey from the string.
     *
     * @param keyString the key string
     * @return SecretKey
     * @throws GeneralSecurityException the general security exception
     */
    
    public static SecretKey getSecretKey(String keyString)
        throws GeneralSecurityException
    {
        byte[] desKeyData = keyString.getBytes();
        
        if (DESKeySpec.isWeak(desKeyData, 0))
            throw new GeneralSecurityException(
                    SecurityResource.SECRET_KEY_IS_WEAK.getValue());
        
        return SecretKeyFactory.getInstance("DES").generateSecret(
                new DESKeySpec(desKeyData));
    }
}
