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

import rye.jdbc.driver.RyeErrorCode;
import rye.jdbc.driver.RyeException;

public class BindParameter
{
    private int count;
    private Object[] values;
    private boolean isBinded[];

    public BindParameter(int parameterCount)
    {
	count = parameterCount;
	values = new Object[count];
	isBinded = new boolean[count];

	clear();
    }

    public boolean checkAllBinded()
    {
	for (int i = 0; i < count; i++) {
	    if (isBinded[i] == false)
		return false;
	}
	return true;
    }

    public void clear()
    {
	for (int i = 0; i < count; i++) {
	    isBinded[i] = false;
	    values[i] = null;
	}
    }

    public int getCount()
    {
	return count;
    }

    public Object[] getValues()
    {
	return values;
    }
    
    public void setParameter(int index, Object bValue, JciConnection jciCon) throws RyeException
    {
	if (index <= 0 || index > count)
	    throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_INVALID_INDEX, null);

	values[index - 1] = bValue;

	isBinded[index - 1] = true;
    }

    void writeParameter(OutputBuffer outBuffer) throws JciException
    {
	try {
	    for (int i = 0; i < count; i++) {
		outBuffer.writeParameter(values[i]);
	    }
	} catch (IOException e) {
	    throw new JciException(RyeErrorCode.ER_INVALID_ARGUMENT);
	}
    }
}
