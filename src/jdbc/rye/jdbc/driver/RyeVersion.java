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

package rye.jdbc.driver;

public class RyeVersion
{
    private short major;
    private short minor;
    private short patch;
    private short build;

    public RyeVersion()
    {
	set((short) 0, (short) 0, (short) 0, (short) 0);
    }

    public RyeVersion(short major, short minor, short patch, short build)
    {
	set(major, minor, patch, build);
    }

    public void set(short major, short minor, short patch, short build)
    {
	this.major = major;
	this.minor = minor;
	this.patch = patch;
	this.build = build;
    }

    public short getMajor()
    {
	return major;
    }

    public short getMinor()
    {
	return minor;
    }

    public short getPatch()
    {
	return patch;
    }

    public short getBuild()
    {
	return build;
    }

    public long getProtocolVersion()
    {
	return RyeVersion.getProtocolVersion(major, minor, patch, build);
    }

    public static long getProtocolVersion(short verMajor, short verMinor, short verPatch, short verBuild)
    {
	return ((long) verMajor << 48 | (long) verMinor << 32 | (long) verPatch << 16 | (long) verBuild);
    }
}
