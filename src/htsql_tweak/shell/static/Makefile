.PHONY: clean

JQUERY=http://code.jquery.com/jquery-1.6.4.min.js
CODEMIRROR=https://nodeload.github.com/marijnh/CodeMirror2/zipball/v2.13

external:
	mkdir external
	mkdir external/jquery
	wget -q -P external/jquery ${JQUERY}
	wget -q -P external --content-disposition ${CODEMIRROR}
	unzip -q external/marijnh-CodeMirror2-*.zip -d external
	rm external/marijnh-CodeMirror2-*.zip
	mv external/marijnh-CodeMirror2-* external/codemirror

clean:
	rm -rf external

