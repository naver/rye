/*
 * Copyright 2017 NAVER Corp.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright notice, this list of conditions and the
 * following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided with the distribution.
 *  - Neither the name of Search Solution Coporation nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package rye.jdbc.admin;

import java.io.PrintStream;

public class Shard
{
    private final static String className = Shard.class.getName();

    public static void main(String[] args) throws Exception
    {
	if (run(args, System.out, System.err) == false) {
	    System.exit(1);
	}
    }

    public static boolean run(String[] args, PrintStream out, PrintStream err)
    {
	ShardCommand command = null;
	ShardCommand[] allCommands = { new ShardInit(), new ShardNodeAdd(), new ShardInstanceAdd(), new ShardNodeDrop(),
			new ShardInstanceDrop(), new ShardRebalance(), new ShardChangeConf(), new ShardInfo() };

	for (int i = 0; i < allCommands.length; i++) {
	    if (allCommands[i].commandName().equalsIgnoreCase(args.length > 0 ? args[0] : null)) {
		command = allCommands[i];
		break;
	    }
	}

	if (command == null) {
	    if (err != null) {
		err.printf("rye shard utility%n");
		err.printf("usage: java %s <command> [args]%n%n", className);
		err.printf("available commands:%n");
		for (int i = 0; i < allCommands.length; i++) {
		    err.printf("    %s%n", allCommands[i].commandName());
		}
		err.printf("%n");
	    }
	    return false;
	}

	command.setErrStream(err);

	try {
	    int numOptArgs = 0;
	    int numArgs = 0;
	    for (int i = 1; i < args.length; i++) {
		if (args[i].startsWith("-")) {
		    numOptArgs++;
		}
		else {
		    numArgs++;
		}
	    }
	    String[] optArgs = new String[numOptArgs];
	    String[] commandArgs = new String[numArgs];
	    numOptArgs = numArgs = 0;
	    for (int i = 1; i < args.length; i++) {
		if (args[i].startsWith("-")) {
		    optArgs[numOptArgs++] = args[i];
		}
		else {
		    commandArgs[numArgs++] = args[i];
		}
	    }

	    command.getArgs(optArgs, commandArgs, out);
	} catch (Exception e) {
	    if (err != null) {
		err.println(e);
		command.printUsage(err, className);
	    }
	    command.clear();
	    return false;
	}

	try {
	    return command.run();
	} catch (Exception e) {
	    if (err != null) {
		e.printStackTrace(err);
	    }
	    return false;
	} finally {
	    command.clear();
	}
    }
}
