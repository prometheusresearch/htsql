.PHONY: clean

external:
	mkdir external
	mkdir external/reset
	wget -q -P external/reset http://meyerweb.com/eric/tools/css/reset/reset.css
	mkdir external/jquery
	wget -q -P external/jquery http://code.jquery.com/jquery-1.6.2.min.js
	wget -q -P external http://codemirror.net/codemirror.zip
	unzip -q external/codemirror.zip -d external
	rm external/codemirror.zip
	mv external/CodeMirror-* external/codemirror

clean:
	rm -rf external

