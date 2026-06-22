MAIN_CLASS = Main
DEPS = lib/dependency

DEPENDENCIES := lib
$(DEPENDENCIES):
	mvn -q dependency:copy-dependencies -DoutputDirectory=$(DEPS) 2> /dev/null

.PHONY: all
all: build

.PHONY: build
build: $(JAVA_SOURCE) $(DEPENDENCIES)
	javac -cp "$(DEPS)/*" *.java

.PHONY: run
run:
	java -XX:+UseParallelGC -cp ".:$(DEPS)/*" $(MAIN_CLASS) $(ARGS)

.PHONY: clean
clean:
	rm -f *.class
	rm -rf
