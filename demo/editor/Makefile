.PHONY: clean

build:
	mkdir build
	wget -q -P build http://codemirror.net/codemirror.zip
	unzip -q build/codemirror.zip -d build
	rm build/codemirror.zip
	mv build/CodeMirror-* build/codemirror

clean:
	rm -rf build

