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

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeType;
import rye.jdbc.jci.ByteArrayBuffer;

class OutputBuffer
{
    private JciConnection jciCon;
    private OutputStream output;
    private ByteArrayBuffer dataBuffer;

    OutputBuffer(JciConnection con)
    {
	this.jciCon = con;
	dataBuffer = new ByteArrayBuffer();
    }

    void clear()
    {
	jciCon = null;
	output = null;
	initBuffer();
    }

    private void initBuffer()
    {
	dataBuffer.reset();
    }

    void sendData(byte[] info) throws IOException
    {
	dataBuffer.writeToStream(info, output);
	initBuffer();
    }

    void newRequest(OutputStream out, byte func_code) throws IOException
    {
	output = out;
	initBuffer();
	dataBuffer.writeByte(func_code);
    }

    int addInt(int intValue) throws IOException
    {
	dataBuffer.writeInt(4);
	dataBuffer.writeInt(intValue);
	return 8;
    }

    int addLong(long longValue) throws IOException
    {
	dataBuffer.writeInt(8);
	dataBuffer.writeLong(longValue);
	return 12;
    }

    int addByte(byte bValue) throws IOException
    {
	dataBuffer.writeInt(1);
	dataBuffer.writeByte(bValue);
	return 5;
    }

    int addBytes(byte[] value) throws IOException
    {
	return addBytes(value, 0, value.length);
    }

    int addBytes(byte[] value, int offset, int len) throws IOException
    {
	dataBuffer.writeInt(len);
	dataBuffer.write(value, offset, len);
	return len + 4;
    }

    int addNull() throws IOException
    {
	dataBuffer.writeInt(0);
	return 4;
    }

    int addString(String str, Charset charset) throws IOException
    {
	byte[] b;

	if (charset == null) {
	    charset = jciCon.getCharset();
	}

	b = str.getBytes(charset);

	dataBuffer.writeInt(b.length + 1);
	dataBuffer.write(b, 0, b.length);
	dataBuffer.writeByte((byte) 0);
	return b.length + 5;
    }

    int addStringWithNull(String str) throws IOException
    {
	byte[] b = str.getBytes(jciCon.getCharset());

	dataBuffer.writeInt(b.length + 1);
	dataBuffer.write(b, 0, b.length);
	dataBuffer.writeByte((byte) 0);
	return b.length + 5;
    }

    int addDouble(double value) throws IOException
    {
	dataBuffer.writeInt(8);
	dataBuffer.writeDouble(value);
	return 12;
    }

    int addShort(short value) throws IOException
    {
	dataBuffer.writeInt(2);
	dataBuffer.writeShort(value);
	return 6;
    }

    int addFloat(float value) throws IOException
    {
	dataBuffer.writeInt(4);
	dataBuffer.writeFloat(value);
	return 8;
    }

    int addDate(Date value) throws IOException
    {
	dataBuffer.writeInt(14);
	writeDate(value);
	return 18;
    }

    private Calendar c = Calendar.getInstance();

    private void writeDate(Date date) throws IOException
    {
	c.setTime(date);
	dataBuffer.writeShort(c.get(Calendar.YEAR));
	dataBuffer.writeShort(c.get(Calendar.MONTH) + 1);
	dataBuffer.writeShort(c.get(Calendar.DAY_OF_MONTH));
	dataBuffer.writeShort((short) 0);
	dataBuffer.writeShort((short) 0);
	dataBuffer.writeShort((short) 0);
	dataBuffer.writeShort((short) 0);
    }

    int addTime(Time value) throws IOException
    {
	dataBuffer.writeInt(14);
	writeTime(value);
	return 18;
    }

    private void writeTime(Time date) throws IOException
    {
	c.setTime(date);
	dataBuffer.writeShort((short) 0);
	dataBuffer.writeShort((short) 0);
	dataBuffer.writeShort((short) 0);
	dataBuffer.writeShort(c.get(Calendar.HOUR_OF_DAY));
	dataBuffer.writeShort(c.get(Calendar.MINUTE));
	dataBuffer.writeShort(c.get(Calendar.SECOND));
	dataBuffer.writeShort((short) 0);
    }

    int addTimestamp(Timestamp value) throws IOException
    {
	dataBuffer.writeInt(14);
	writeTimestamp(value, false);
	return 18;
    }

    private void writeTimestamp(Timestamp date, boolean withMili) throws IOException
    {
	c.setTime(date);
	dataBuffer.writeShort(c.get(Calendar.YEAR));
	dataBuffer.writeShort(c.get(Calendar.MONTH) + 1);
	dataBuffer.writeShort(c.get(Calendar.DAY_OF_MONTH));
	dataBuffer.writeShort(c.get(Calendar.HOUR_OF_DAY));
	dataBuffer.writeShort(c.get(Calendar.MINUTE));
	dataBuffer.writeShort(c.get(Calendar.SECOND));
	if (withMili) {
	    dataBuffer.writeShort(c.get(Calendar.MILLISECOND));
	}
	else {
	    dataBuffer.writeShort((short) 0);
	}
    }

    int addDatetime(Timestamp value) throws IOException
    {
	dataBuffer.writeInt(14);
	writeTimestamp(value, true);
	return 18;
    }

    int writeParameter(Object value) throws JciException, IOException
    {
	byte type = RyeType.getObjectDBtype(value);
	addByte(type);

	switch (type)
	{
	case RyeType.TYPE_NULL:
	    return addNull();
	case RyeType.TYPE_VARCHAR:
	    return addStringWithNull((String) value);
	case RyeType.TYPE_NUMERIC:
	    return addStringWithNull(((BigDecimal) value).toPlainString());
	case RyeType.TYPE_BINARY:
	    return addBytes((byte[]) value);
	case RyeType.TYPE_DOUBLE:
	    return addDouble(((Number) value).doubleValue());
	case RyeType.TYPE_DATE:
	    return addDate((Date) value);
	case RyeType.TYPE_TIME:
	    return addTime((Time) value);
	case RyeType.TYPE_DATETIME:
	    return addDatetime((Timestamp) value);
	case RyeType.TYPE_INT:
	    return addInt(((Number) value).intValue());
	case RyeType.TYPE_BIGINT:
	    return addLong(((Long) value).longValue());
	default:
	    throw JciException.createJciException(jciCon, RyeErrorCode.ER_TYPE_CONVERSION);
	}
    }
}
