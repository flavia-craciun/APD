JAVAC=javac
sources = $(wildcard *.java)
classes = $(sources:.java=.class)

build: $(classes)

clean:
	rm -f *.class

%.class: %.java
	$(JAVAC) $<

.PHONY: build clean
