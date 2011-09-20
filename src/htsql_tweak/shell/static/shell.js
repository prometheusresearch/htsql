
$(document).ready(function() {

    function log(data) {
        if (console)
            console.log(data);
    }

    function timing(point) {
        if (!Date.now)
            return;
        var timestamp = Date.now();
        if (state.lastTimestamp) {
            var delta = (timestamp-state.lastTimestamp)/1000;
            log("("+state.lastPoint+") -> ("+point+"): "+delta);
        }
        state.lastPoint = point;
        state.lastTimestamp = timestamp;
    }

    function makeConfig() {
        var $body = $('body');
        return {
            databaseName: $body.attr('data-database-name') || "",
            serverName: $body.attr('data-server-name') || "",
            serverRoot: $body.attr('data-server-root') || "",
            queryOnStart: $body.attr('data-query-on-start') || "/",
            evaluateOnStart: ($body.attr('data-evaluate-on-start') == 'true'),
            implicitShell: ($body.attr('data-implicit-shell') == 'true')
        };
    }

    function makeEnviron() {
        var $outer = $("<div></div>").prependTo($('body'))
                        .css({ position: 'absolute',
                               left: 0, top: -1000,
                               width: 100, height: 100,
                               overflow: 'auto' });
        var $inner = $("<div></div>").prependTo($outer)
                        .css({ width: '100%', height: 1000 });
        var scrollbarWidth = 100-$inner.width();
        $outer.remove();
        return {
            scrollbarWidth: scrollbarWidth || 20,
            screenWidth: window.screen.width || 2000,
        }
    }

    function makeState() {
        return {
            $panel: null,
            waiting: false,
            lastQuery: null,
            lastAction: null,
            lastPage: null,
            lastOffset: null,
            marker: null,
            expansion: 0,
            lastPoint: null,
            lastTimestamp: null
        }
    }

    function makeEditor() {
        var $editor = $('#editor');
        var editor = CodeMirror.fromTextArea($editor[0], {
            mode: 'htsql',
            onKeyEvent: function(i, e) {
                // Ctrl-Enter
                if (e.ctrlKey && e.keyCode == 13) {
                    if (e.type == 'keyup') {
                        $('#run').click();
                    }
                    return true;
                }
                // Ctrl-Up
                if (e.ctrlKey && e.keyCode == 38) {
                    if (e.type == 'keyup') {
                        $('#shrink').click();
                    }
                    return true;
                }
                // Ctrl-Down
                if (e.ctrlKey && e.keyCode == 40) {
                    if (e.type == 'keyup') {
                        $('#expand').click();
                    }
                    return true;
                }
            }
        });
        return editor;
    }

    function updateTitle(message) {
        if (!message) {
            message = '['+config.databaseName+']';
        }
        else {
            message = message+' ['+config.databaseName+']';
        }
        $('title').text(message);
    }

    function setQuery(query) {
        editor.setValue(query);
        editor.setCursor(editor.lineCount(), 0);
    }

    function getQuery() {
        var orig_query = editor.getValue();
        var query = orig_query.replace(/^\s+|\s+$/g, "");
        if (query != orig_query) {
            editor.setValue(query);
        }
        if (!/^\//.test(query)) {
            return;
        }
        return query;
    }

    function clickSchema() {
    }

    function clickHelp() {
    }

    function clickRun() {
        var query = getQuery();
        pushHistory(query)
        run(query);
    }

    function clickExpand(dir) {
        var expansion = state.expansion+dir;
        if (expansion < -1 || expansion > 3)
            return;
        if (state.expansion == -1)
            $shrink.css({ visibility: 'visible' });
        if (expansion == -1)
            $shrink.css({ visibility: 'hidden' });
        if (state.expansion == 3)
            $expand.css({ visibility: 'visible' });
        if (expansion == 3)
            $expand.css({ visibility: 'hidden' });
        var height = null;
        if (expansion < 0)
            height = 4;
        else
            height = 8*(expansion+1);
        $('.input-area').css({ height: height+'em' });
        $('.output-area').css({ top: (height+2)+'em' });
        state.expansion += dir;
    }

    function clickPopups() {
        $popups.hide();
        $popups.children('.popup').hide();
    }

    function clickMore() {
        var $more = $('#more');
        var offset = $more.offset();
        $morePopup.css({ top: offset.top+$more.outerHeight(),
                         right: $viewport.width()-offset.left-$more.outerWidth() });
        $popups.show();
        $morePopup.show();
    }

    function clickExport(format) {
        $popups.hide();
        $popups.children('.popup').hide();
        var query = getQuery();
        if (query && query != '/') {
            query += '/:'+format;
            run(query);
        }
    }

    function clickShowSql() {
        $popups.hide();
        $popups.children('.popup').hide();
        var query = getQuery();
        if (query && query != '/') {
            run(query, 'analyze');
        }
    }

    function clickLoad() {
        var query = state.lastQuery
        var page = state.lastPage || 1;
        state.lastOffset = $gridBody.scrollTop();
        run(query, 'produce', page+1);
    }

    function clickClose() {
        if (state.$panel) {
            state.$panel.hide();
            state.$panel = null;
        }
    }

    function scrollGrid() {
        $gridHead.scrollLeft($gridBody.scrollLeft());
    }

    function popstateWindow(event) {
        var data = event.originalEvent.state;
        if (!data) {
            return;
        }
        var query = data.query;
        if (query == getQuery()) {
            return;
        }
        if (state.waiting) {
            return
        }
        setQuery(query);
        if (state.$panel)
            state.$panel.hide();
        state.$panel = null;
        updateTitle();
    }

    function pushHistory(query, replace) {
        if (!history.pushState || !history.replaceState)
            return;
        if (!query || !config.serverRoot || state.waiting)
            return;
        var data = { query: query };
        var title = 'HTSQL';
        if (!config.implicitShell) {
            if (query == '/')
                query = "/shell()";
            else
                query = "/shell('" + query.replace(/'/g,"''") + "')";
        }
        var url = config.serverRoot+escape(query);
        if (replace)
            history.replaceState(data, title, url);
        else
            history.pushState(data, title, url);
    }

    function run(query, action, page) {
        if (!query)
            return;
        if (!config.serverRoot)
            return;
        if (state.waiting)
            return;
        if (state.marker) {
            state.marker();
            state.marker = null;
        }
        state.lastQuery = query;
        state.lastAction = action;
        state.lastPage = page;
        if (!action)
            action = 'produce';
        if (!page)
            page = 1;
        query = "/evaluate('" + query.replace(/'/g,"''") + "'"
                + ",'" + action + "'" + "," + page + ")";
        var url = config.serverRoot+escape(query);
        $.ajax({
            url: url,
            dataType: 'json',
            success: handleSuccess,
            error: handleFailure,
        });
        state.waiting = true;
        setTimeout(showWaiting, 1000);
    }

    function handleFailure() {
        state.waiting = false;
        if (state.$panel)
            state.$panel.hide();
        state.$panel = $failurePanel.show();
        updateTitle();
    }

    function handleSuccess(output) {
        state.waiting = false;
        switch (output.type) {
            case "product":
                handleProduct(output);
                break;
            case "empty":
                handleEmpty(output);
                break;
            case "sql":
                handleSql(output);
                break;
            case "error":
                handleError(output);
                break;
            case "unsupported":
                handleUnsupported(output);
        }
    }

    function handleError(output) {
        if (state.$panel)
            state.$panel.hide();
        state.$panel = $errorPanel.show();
        $error.html(output.detail);
        if (state.marker) {
            state.marker();
            state.marker = null;
        }
        if (output.first_line !== null && output.first_column !== null &&
                output.last_line !== null && output.last_column !== null) {
            editor.setCursor({ line: output.first_line,
                               ch: output.first_column });
            state.marker = editor.markText({ line: output.first_line,
                                             ch: output.first_column },
                                           { line: output.last_line,
                                             ch: output.last_column },
                                           'marker');
        }
        updateTitle($error.text());
    }

    function handleUnsupported(output) {
        if (state.$panel)
            state.$panel.hide();
        state.$panel = null;
        if (!state.lastAction) {
            var url = config.serverRoot+escape(state.lastQuery);
            window.open(url, "_blank");
        }
        updateTitle();
    }

    function handleEmpty(output) {
        if (state.$panel)
            state.$panel.hide();
        state.$panel = null;
        updateTitle();
    }

    function handleSql(output) {
        if (state.$panel)
            state.$panel.hide();
        state.$panel = $sqlPanel.show();
        $sql.html(output.sql);
        updateTitle();
    }

    function handleProduct(output) {
//        timing("rendering table");
        var style = output.style;
        var head = output.head;
        var body = output.body;
        var size = style.length;
        var width = $viewport.width();
        var constraint = null;
        if (width < 640)
            constraint = 'w640';
        else if (width < 800)
            constraint = 'w800';
        else if (width < 1024)
            constraint = 'w1024';
        else if (width < 1280)
            constraint = 'w1280';
        else if (width < 1600)
            constraint = 'w1600';
        var table = '';
        table += '<table'+(constraint ? ' class="'+constraint+'"' : '')+'>';
        table += '<colgroup>';
        table += '<col>';
        for (var i = 0; i < size; i ++) {
            table += '<col>';
        }
        table += '<col style="width: '+width+'px">';
        table += '</colgroup>';
        table += '<thead>';
        for (var k = 0; k < head.length; k ++) {
            var row = head[k];
            table += '<tr>';
            if (k == 0) {
                table += '<th' + (head.length > 1 ? ' rowspan="'+head.length+'"' : '')
                              + ' class="dummy">&nbsp;</th>';
            }
            for (var i = 0; i < head[k].length; i ++) {
                var cell = row[i];
                var title = cell[0];
                var colspan = cell[1];
                var rowspan = cell[2];
                table += '<th' + (colspan>1 ? ' colspan="'+colspan+'"' : '')
                                    + (rowspan>1 ? ' rowspan="'+rowspan+'"' : '') + '>'
                              + title + '</th>';
            }
            if (k == 0) {
                table += '<th' + (head.length > 1 ? ' rowspan="'+head.length+'"' : '')
                              + ' class="dummy">&nbsp;</th>';
            }
            table += '</tr>';
        }
        table += '</thead>';
        table += '<tbody>';
        for (var k = 0; k < body.length; k ++) {
            var row = body[k];
            table += '<tr' + (k % 2 == 1 ? ' class="alt">' : '>');
            table += '<th>'+(k+1)+'</th>';
            for (var i = 0; i < size; i ++) {
                var value = row[i];
                if (value == null) {
                    value = '&nbsp;'
                }
                table += '<td' + (style[i] ? ' class="'+style[i]+'">' : '>')
                              + value + '</td>';
            }
            table += '<td class="dummy">&nbsp;</td>';
            table += '</tr>';
        }
        var ch = output.more ? "&#x22EE;" : "&nbsp;"
        table += '<tr class="dummy">';
        table += '<th class="dummy">'+ch+'</th>';
        for (var i = 0; i < size; i ++) {
            table += '<td class="dummy">'+ch+'</td>';
        }
        table += '<td class="dummy">&nbsp;</td>';
        table += '</tbody>';
        table += '</table>';
        if (output.more) {
            table += '<button id="load">Load More Data</button>';
        }
        $gridHead.empty();
        $gridBody.empty()
            .css({ top: 0,
                   width: size*width+"px",
                   right: "auto",
                   "overflow-y": "hidden",
                   "overflow-x": "hidden" });
        if (state.$panel)
            state.$panel.hide();
        state.$panel = $productPanel.show();
        var title = '';
        if (head.length > 0) {
            for (var i = 0; i < head[0].length; i ++) {
                if (title)
                    title += ', ';
                title += head[0][i][0].replace(/&lt;/g, '<')
                                      .replace(/&gt;/g, '>')
                                      .replace(/&amp;/g, '&');
            }
        }
        updateTitle(title);
        $gridBody.html(table);
        $gridBody.scrollLeft(0).scrollTop(0);
        if (state.lastPage && state.lastOffset) {
            $gridBody.scrollTop(state.lastOffset);
            state.lastOffset = 0;
        }
        $('#load').click(clickLoad);
        setTimeout(configureGrid, 0);
    }

    function configureGrid() {
        var $bodyTable = $gridBody.children('table');
        var gridWidth = $grid.width();
        var colWidths = [];
        $bodyTable.find('tbody tr:first-child').children().each(function(idx) {
            colWidths[idx] = $(this).outerWidth();
        });
        colWidths[colWidths.length-1] = 1;
        var tableWidth = 0;
        for (var i = 0; i < colWidths.length; i ++) {
            tableWidth += colWidths[i];
        }
        var overflow = 'auto';
        if (tableWidth < gridWidth) {
            var diff = gridWidth-tableWidth-1;
            colWidths[colWidths.length-1] += diff;
            tableWidth += diff;
            if (diff >= 2*environ.scrollbarWidth)
                overflow = 'hidden';
        }
        var $bodyGroup = $("<colgroup></colgroup>");
        var $headGroup = $("<colgroup></colgroup>");
        for (var i = 0; i < colWidths.length; i ++) {
            var width = colWidths[i];
            $bodyGroup.append("<col style=\"width: "+width+"px\">");
            $headGroup.append("<col style=\"width: "+width+"px\">");
        }
        var $headTable = $("<table></table>")
                .appendTo($gridHead)
                .css('table-layout', 'fixed')
                .width(tableWidth)
                .append($headGroup)
                .append($bodyTable.children('thead').clone());
        $gridHead.scrollLeft(0);
        var headHeight = $gridHead.height();
        $("<div></div>").appendTo($gridHead)
            .width(tableWidth+environ.screenWidth)
            .height(1);
        $bodyTable.children('colgroup').remove();
        $bodyTable.children('thead').remove();
        $bodyTable.removeClass();
        $bodyTable.css('table-layout', 'fixed')
                .width(tableWidth)
                .prepend($bodyGroup);
        $gridBody.css({ right: 0,
                        top: headHeight,
                        width: 'auto',
                        'overflow-y': 'auto',
                        'overflow-x': overflow });
        setTimeout(reportTime, 0);
    }

    function reportTime() {
//        timing("!rendering table");
    }

    function showWaiting() {
        if (!state.waiting)
            return;
        if (state.$panel)
            state.$panel.hide();
        state.$panel = $requestPanel.show();
    }

    var config = makeConfig();
    var environ = makeEnviron();
    var state = makeState();

    var $viewport = $('#viewport');
    var $database = $('#database');
    var $productPanel = $('#product-panel');
    var $grid = $('#grid');
    var $gridHead = $('#grid-head');
    var $gridBody = $('#grid-body');
    var $requestPanel = $('#request-panel');
    var $errorPanel = $('#error-panel');
    var $error = $('#error');
    var $failurePanel = $('#failure-panel');
    var $sqlPanel = $('#sql-panel');
    var $sql = $('#sql');
    var $popups = $('#popups');
    var $morePopup = $('#more-popup');
    var $shrink = $('#shrink');
    var $expand = $('#expand');

    var editor = makeEditor();
    setQuery(config.queryOnStart);
    editor.focus();

    $('#schema').click(clickSchema);
    $('#help').click(clickHelp);
    $('#run').click(clickRun);
    $('#more').click(clickMore);
    $('#export-html').click(function() { return clickExport('html'); });
    $('#export-json').click(function() { return clickExport('json'); });
    $('#export-csv').click(function() { return clickExport('csv'); });
    $('#show-sql').click(clickShowSql);
    $('#close-error').click(clickClose);
    $('#close-failure').click(clickClose);
    $('#close-sql').click(clickClose);
    $('#grid-body').scroll(scrollGrid);
    $('#popups').click(clickPopups);
    $('#shrink').click(function() { return clickExpand(-1); });
    $('#expand').click(function() { return clickExpand(+1); });
    $(window).bind('popstate', popstateWindow);

    $('#schema').hide();
    $('#help').hide();
    $('#close-sql').hide();

    $database.text(config.databaseName);
    updateTitle();

    pushHistory(config.queryOnStart, true);
    if (config.evaluateOnStart) {
        run(config.queryOnStart);
    }

});

