# this is my proxyport and pin used locally
export proxyport15440=20080
export pin15440=990105702
export CLASSPATH=${PWD}:${PWD}/../lib
# LD_PRELOAD=../lib/lib440lib.so tools/440read foo

# define compiler and compiler flag variables
JFLAGS = -g
JC = javac

# set necessary environment variables as well
all: Server.class Proxy.class Cache.class Logger.java FileManagerRemote.java ValidateResult.java FileChecker.java

%.class: %.java
	$(JC) $(JFLAGS) $*.java

# format command
.PHONY: format
format:
	# clang-format all the source code files
	find *.java *.cpp | sed 's| |\\ |g' | xargs clang-format -style=google -i

# submission helper command
.PHONY: submit
submit:
	# submit by compressing tar
	tar cvzf ../mysolution.tgz Makefile Server.java Proxy.java Cache.java Logger.java FileChecker.java FileManagerRemote.java ValidateResult.java

# clean up command
.PHONY: clean
clean:
	# remove all the files with extension .class and executable with tar
	rm -f *.class ../mysolution.tgz tester.txt*

# personal tester build
.PHONY: tester
tester:
	# remove all the files with extension .class and executable with tar
	g++ -std=c++11 -o ../tools/tester tester.cpp