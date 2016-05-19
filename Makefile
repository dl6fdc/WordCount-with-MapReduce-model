JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) -cp .:../zookeeper-3.4.6/zookeeper-3.4.6.jar $*.java

CLASSES = \
	WordCount.java \
	DeleteZK.java \

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class *~

