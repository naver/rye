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
import java.util.ArrayList;
import java.util.Iterator;

class ByteArrayBuffer
{
    private static final int UnitSize = 102400;

    private ArrayList<byte[]> byteArrayList;
    private byte[] baseByteArray;
    private byte[] buffer;
    private int pos;
    private int dataSize;
    private byte writeLongBuffer[];

    ByteArrayBuffer()
    {
	baseByteArray = new byte[UnitSize];
	byteArrayList = new ArrayList<byte[]>();
	writeLongBuffer = new byte[8];
	reset();
    }

    void writeByte(byte v)
    {
	write(v);
    }

    void writeShort(int v) throws IOException
    {
	write((v >>> 8) & 0xFF);
	write((v >>> 0) & 0xFF);
    }

    void writeInt(int v)
    {
	write((v >>> 24) & 0xFF);
	write((v >>> 16) & 0xFF);
	write((v >>> 8) & 0xFF);
	write((v >>> 0) & 0xFF);
    }

    void writeLong(long v)
    {
	writeLongBuffer[0] = (byte) (v >>> 56);
	writeLongBuffer[1] = (byte) (v >>> 48);
	writeLongBuffer[2] = (byte) (v >>> 40);
	writeLongBuffer[3] = (byte) (v >>> 32);
	writeLongBuffer[4] = (byte) (v >>> 24);
	writeLongBuffer[5] = (byte) (v >>> 16);
	writeLongBuffer[6] = (byte) (v >>> 8);
	writeLongBuffer[7] = (byte) (v >>> 0);
	write(writeLongBuffer, 0, 8);
    }

    void writeFloat(float v) throws IOException
    {
	writeInt(Float.floatToIntBits(v));
    }

    void writeDouble(double v) throws IOException
    {
	writeLong(Double.doubleToLongBits(v));
    }

    void write(byte[] b, int off, int len)
    {
	if (b == null) {
	    throw new NullPointerException();
	}
	else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
	    throw new IndexOutOfBoundsException();
	}
	else if (len == 0) {
	    return;
	}
	for (int i = 0; i < len; i++) {
	    write(b[off + i]);
	}
    }

    private void write(int b)
    {
	if (pos == UnitSize) {
	    byteArrayList.add(buffer);
	    buffer = new byte[UnitSize];
	    pos = 0;
	}
	buffer[pos] = (byte) b;
	pos++;
	dataSize++;
    }

    private void writeHeader(byte[] info)
    {
	byte[] header = null;

	if (byteArrayList.size() == 0) {
	    header = buffer;
	}
	else {
	    header = byteArrayList.get(0);
	}
	header[0] = (byte) ((dataSize >>> 24) & 0xFF);
	header[1] = (byte) ((dataSize >>> 16) & 0xFF);
	header[2] = (byte) ((dataSize >>> 8) & 0xFF);
	header[3] = (byte) ((dataSize >>> 0) & 0xFF);
	System.arraycopy(info, 0, header, 4, Protocol.CAS_STATUS_INFO_SIZE);
    }

    void writeToStream(byte[] info, OutputStream outStream) throws IOException
    {
	writeHeader(info);

	Iterator<byte[]> i = byteArrayList.iterator();
	while (i.hasNext()) {
	    byte[] b = i.next();
	    if (b != null) {
		outStream.write(b);
	    }
	}
	outStream.write(buffer, 0, pos);
	outStream.flush();
    }

    void reset()
    {
	byteArrayList.clear();
	buffer = baseByteArray;
	pos = Protocol.MSG_HEADER_SIZE; // message header size: msg length + CAS_STATUS_INFO_SIZE
	dataSize = 0;
    }

}
