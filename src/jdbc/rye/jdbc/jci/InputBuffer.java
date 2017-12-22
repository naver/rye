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
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import rye.jdbc.driver.ConnectionProperties;
import rye.jdbc.driver.RyeErrorCode;

public class InputBuffer
{
    private int position;
    private int capacity;
    private byte buffer[];
    private JciConnection jciCon;
    private final Charset charset;

    InputBuffer(TimedDataInputStream inStream, JciConnection con, CasInfo casInfo, Charset charset) throws IOException,
		    JciException
    {
	position = 0;
	jciCon = con;
	this.charset = charset;

	byte[] headerData = new byte[Protocol.MSG_HEADER_SIZE];
	inStream.readFully(headerData);
	capacity = JciUtil.bytes2int(headerData, 0);
	casInfo.setStatusInfo(headerData, 4, Protocol.CAS_STATUS_INFO_SIZE);

	if (capacity <= 0) {
	    capacity = 0;
	}

	buffer = new byte[capacity];
	inStream.readFully(buffer);

	byte response = readByte();
	if (response == Protocol.ERROR_RESPONSE) {
	    int errIndicator = readInt();
	    int errCode = readInt();
	    int errMsgLen = readInt();
	    String errMsg = readString(errMsgLen);
	    throw new JciException(RyeErrorCode.ER_DBMS, errIndicator, errCode, errMsg);
	}
    }

    public InputBuffer(byte[] netStream, Charset charset)
    {
	position = 0;
	jciCon = null;
	capacity = netStream.length;
	buffer = netStream;
	this.charset = charset;
    }

    public boolean readBoolean() throws JciException
    {
	if (readByte() == (byte) 0) {
	    return false;
	}
	else {
	    return true;
	}
    }

    public byte readByte() throws JciException
    {
	if (position >= capacity) {
	    throw JciException.createJciException(jciCon, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
	}

	return buffer[position++];
    }

    public void readBytes(byte value[], int offset, int len) throws JciException
    {
	if (value == null)
	    return;

	if (position + len > capacity) {
	    throw JciException.createJciException(jciCon, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
	}

	System.arraycopy(buffer, position, value, offset, len);
	position += len;
    }

    public void readBytes(byte value[]) throws JciException
    {
	readBytes(value, 0, value.length);
    }

    public byte[] readBytes(int size) throws JciException
    {
	byte[] value = new byte[size];
	readBytes(value, 0, size);
	return value;
    }

    public double readDouble() throws JciException
    {
	return Double.longBitsToDouble(readLong());
    }

    public float readFloat() throws JciException
    {
	return Float.intBitsToFloat(readInt());
    }

    public int readInt() throws JciException
    {
	if (position + 4 > capacity) {
	    throw JciException.createJciException(jciCon, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
	}

	int data = JciUtil.bytes2int(buffer, position);
	position += 4;

	return data;
    }

    public long readLong() throws JciException
    {
	long data = 0;

	if (position + 8 > capacity) {
	    throw JciException.createJciException(jciCon, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
	}

	for (int i = 0; i < 8; i++) {
	    data <<= 8;
	    data |= (buffer[position++] & 0xff);
	}

	return data;
    }

    public short readShort() throws JciException
    {
	if (position + 2 > capacity) {
	    throw JciException.createJciException(jciCon, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
	}

	short data = JciUtil.bytes2short(buffer, position);
	position += 2;

	return data;
    }

    public String readString(int len) throws JciException
    {
	if (len <= 0) {
	    return null;
	}

	if (position + len > capacity) {
	    throw JciException.createJciException(jciCon, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
	}

	String s = new String(buffer, position, len - 1, charset);
	position += len;

	return s;
    }

    public String readString(int size, Charset applyCharset) throws JciException
    {
	String stringData;

	if (size <= 0)
	    return null;

	if (position + size > capacity) {
	    throw JciException.createJciException(jciCon, RyeErrorCode.ER_ILLEGAL_DATA_SIZE);
	}

	stringData = new String(buffer, position, size - 1, applyCharset);

	position += size;

	return stringData;
    }

    Date readDate() throws JciException
    {
	int year, month, day;
	year = readShort();
	month = readShort();
	day = readShort();

	if (year == 0 && month == 0 && day == 0) {
	    if (jciCon.getZeroDateTimeBehavior() == ConnectionProperties.ZERO_DATE_BEHAVIOR.EXCEPTION) {
		throw new JciException(RyeErrorCode.ER_ILLEGAL_TIMESTAMP);
	    }
	    else if (jciCon.getZeroDateTimeBehavior() == ConnectionProperties.ZERO_DATE_BEHAVIOR.CONVERT_TO_NULL) {
		return null;
	    }
	}

	Calendar cal = Calendar.getInstance();
	if (year == 0 && month == 0 && day == 0) {
	    cal.set(0, 0, 1, 0, 0, 0); /* round to 0001-01-01 00:00:00) */
	}
	else {
	    cal.set(year, month - 1, day, 0, 0, 0);
	}
	cal.set(Calendar.MILLISECOND, 0);

	return new Date(cal.getTimeInMillis());
    }

    Time readTime() throws JciException
    {
	int hour, minute, second;
	hour = readShort();
	minute = readShort();
	second = readShort();

	Calendar cal = Calendar.getInstance();
	cal.set(1970, 0, 1, hour, minute, second);
	cal.set(Calendar.MILLISECOND, 0);

	return new Time(cal.getTimeInMillis());
    }

    Timestamp readDatetime() throws JciException
    {
	int year, month, day, hour, minute, second, millisecond;
	year = readShort();
	month = readShort();
	day = readShort();
	hour = readShort();
	minute = readShort();
	second = readShort();
	millisecond = readShort();

	if (year == 0 && month == 0 && day == 0) {
	    if (jciCon.getZeroDateTimeBehavior() == ConnectionProperties.ZERO_DATE_BEHAVIOR.EXCEPTION) {
		throw new JciException(RyeErrorCode.ER_ILLEGAL_TIMESTAMP);
	    }
	    else if (jciCon.getZeroDateTimeBehavior() == ConnectionProperties.ZERO_DATE_BEHAVIOR.CONVERT_TO_NULL) {
		return null;
	    }
	}

	Calendar cal = Calendar.getInstance();
	if (year == 0 && month == 0 && day == 0) {
	    cal.set(0, 0, 1, 0, 0, 0); /* round to 0001-01-01 00:00:00) */
	}
	else {
	    cal.set(year, month - 1, day, hour, minute, second);
	}
	cal.set(Calendar.MILLISECOND, millisecond);

	return new Timestamp(cal.getTimeInMillis());
    }

    int remainedCapacity()
    {
	return capacity - position;
    }
}
