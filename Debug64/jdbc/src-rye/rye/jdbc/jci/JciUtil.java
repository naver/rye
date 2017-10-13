/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */

package rye.jdbc.jci;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

abstract public class JciUtil
{
    public static int bytes2int(byte[] b, int startIndex)
    {
	int data = 0;
	int endIndex = startIndex + 4;

	for (int i = startIndex; i < endIndex; i++) {
	    data <<= 8;
	    data |= (b[i] & 0xff);
	}

	return data;
    }

    static short bytes2short(byte[] b, int startIndex)
    {
	short data = 0;
	int endIndex = startIndex + 2;

	for (int i = startIndex; i < endIndex; i++) {
	    data <<= 8;
	    data |= (b[i] & 0xff);
	}
	return data;
    }

    static long bytes2long(byte[] b, int startIndex)
    {
	long data = 0;
	int endIndex = startIndex + 8;

	for (int i = startIndex; i < endIndex; i++) {
	    data <<= 8;
	    data |= (b[i] & 0xff);
	}
	return data;
    }

    static int short2bytes(short val, byte[] dest, int pos)
    {
	dest[pos] = (byte) ((val >>> 8) & 0xFF);
	dest[pos + 1] = (byte) ((val >>> 0) & 0xFF);
	return (pos + 2);
    }

    static int int2bytes(int val, byte[] dest, int pos)
    {
	dest[pos] = (byte) ((val >>> 24) & 0xFF);
	dest[pos + 1] = (byte) ((val >>> 16) & 0xFF);
	dest[pos + 2] = (byte) ((val >>> 8) & 0xFF);
	dest[pos + 3] = (byte) ((val >>> 0) & 0xFF);
	return (pos + 4);
    }

    static int long2bytes(long val, byte[] dest, int pos)
    {
	dest[pos] = (byte) ((val >>> 56) & 0xFF);
	dest[pos + 1] = (byte) ((val >>> 48) & 0xFF);
	dest[pos + 2] = (byte) ((val >>> 40) & 0xFF);
	dest[pos + 3] = (byte) ((val >>> 32) & 0xFF);
	dest[pos + 4] = (byte) ((val >>> 24) & 0xFF);
	dest[pos + 5] = (byte) ((val >>> 16) & 0xFF);
	dest[pos + 6] = (byte) ((val >>> 8) & 0xFF);
	dest[pos + 7] = (byte) ((val >>> 0) & 0xFF);
	return (pos + 8);
    }

    static int copy_bytes(byte[] dest, int pos, int cpSize, byte[] src)
    {
	if (src == null)
	    return pos;

	cpSize = (cpSize > src.length) ? src.length : cpSize;
	System.arraycopy(src, 0, dest, pos, cpSize);
	return (pos + cpSize);
    }

    static int copy_byte(byte[] dest, int pos, byte src)
    {
	if (dest.length < pos)
	    return pos;

	dest[pos] = src;
	return (pos + 1);
    }

    public static Object invoke(String cls_name, String method, Class<?>[] param_cls, Object cls, Object[] params)
    {
	try {
	    Class<?> c = Class.forName(cls_name);
	    Method m = c.getMethod(method, param_cls);
	    return m.invoke(cls, params);
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}
    }

    public static Constructor<?> getConstructor(String cls_name, Class<?>[] param_cls)
    {
	try {
	    Class<?> c = Class.forName(cls_name);
	    return c.getConstructor(param_cls);
	} catch (Exception e) {
	    return null;
	}
    }

    public static int[] resizeIntArray(int[] arr, int size)
    {
	if (arr.length == size) {
	    return arr;
	}

	int[] tmparr = new int[size];
	int cpsize = (tmparr.length < size ? tmparr.length : size);
	System.arraycopy(arr, 0, tmparr, 0, cpsize);
	return tmparr;
    }
}
