
$(document).ready(function() {

    function log(data) {
        if (console)
            console.log(data);
    }

    function makeConfig() {
        var $body = $('body');
        return {
            databaseName: $body.attr('data-database-name') || "",
            serverRoot: $body.attr('data-server-root') || "",
            queryOnStart: $body.attr('data-query-on-start') || "/",
            evaluateOnStart: ($body.attr('data-evaluate-on-start') == 'true')
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
            scrollbarWidth: scrollbarWidth,
            screenWidth: 2000,
        }
    }

    function makeState() {
        return {
            $panel: null,
            waiting: false,
            lastQuery: null,
            lastAction: null,
            table: null,
            marker: null,
            expansion: 0
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
        var query = editor.getValue();
        query = query.replace(/^\s+|\s+$/g, "");
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

    function clickClose() {
        if (state.$panel) {
            state.$panel.hide();
            state.$panel = null;
        }
    }

    function scrollGrid() {
        $gridHead.scrollLeft($gridBody.scrollLeft());
    }

    function run(query, action) {
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
        query = "/evaluate('" + query.replace(/'/g,"''") + "'"
                + (action ? ",'" + action + "'" : "") + ")";
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
        var style = output.style;
        var head = output.head;
        var body = output.body;
        var size = style.length;
        var table = '';
        table += '<table>';
        table += '<colgroup>';
        table += '<col>';
        for (var i = 0; i < size; i ++) {
            table += '<col>';
        }
        table += '<col style="width: '+environ.screenWidth+'px">';
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
                              + '<span>' + title + '</span></th>';
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
            table += '<th><span>'+(k+1)+'</span></th>';
            for (var i = 0; i < size; i ++) {
                var value = row[i];
                if (value == null) {
                    value = '&nbsp;'
                }
                table += '<td><span' + (style[i] ? ' class="'+style[i]+'">' : '>')
                              + value + '</span></td>';
            }
            table += '<td class="dummy">&nbsp;</td>';
            table += '</tr>';
        }
        table += '<tr>';
        table += '<th class="dummy">&nbsp;</th>';
        for (var i = 0; i < size; i ++) {
            table += '<td class="dummy">&nbsp;</td>';
        }
        table += '<td class="dummy">&nbsp;</td>';
        table += '</tbody>';
        table += '</table>';
        state.table = table;
        $grid.removeAttr('style');
        $gridHead.removeAttr('style').empty();
        $gridBody.removeAttr('style').empty();
        if (state.$panel)
            state.$panel.hide();
        state.$panel = $productPanel.show();
        setTimeout(addTable, 0);
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
    }

    function showWaiting() {
        if (!state.waiting)
            return;
        if (state.$panel)
            state.$panel.hide();
        state.$panel = $requestPanel.show();
    }

    function addTable() {
        $gridBody.css({ width: ($grid.width()+environ.screenWidth-1)+"px",
                        right: "auto" });
        $gridBody.html(state.table);
        setTimeout(splitTable, 0);
    }

    function splitTable() {
        var colWidths = [];
        var headHeight = $gridBody.find('thead').height();
        $gridBody.find('tbody').find('tr:first-child').children().each(function(idx) {
            colWidths[idx] = $(this).width();
        });
        colWidths[colWidths.length-1] = 1;
        $gridBody.find('colgroup').remove()
        $("<table></table>").appendTo($gridHead).append($gridBody.find('thead'));
        var tableWidth = 0;
        for (var i = 0; i < colWidths.length-1; i ++) {
            tableWidth += colWidths[i];
        }
        var panelWidth = $productPanel.width();
        if (tableWidth < panelWidth) {
            var diff = panelWidth-tableWidth-1;
            colWidths[colWidths.length-1] += diff;
            tableWidth += diff;
        }
        var $bodyTable = $gridBody.children('table');
        var $headTable = $gridHead.children('table');
        var $bodyGroup = $("<colgroup></colgroup>");
        var $headGroup = $("<colgroup></colgroup>");
        for (var i = 0; i < colWidths.length; i ++) {
            var width = colWidths[i];
            $bodyGroup.append("<col style=\"width: "+width+"px\">");
            if (i == colWidths.length-1)
                width += environ.screenWidth;
            $headGroup.append("<col style=\"width: "+width+"px\">");
        }
        $bodyTable.prepend($($bodyGroup))
            .width(tableWidth)
            .css('table-layout', 'fixed');
        $headTable.prepend($($headGroup))
            .width(tableWidth+environ.screenWidth)
            .css('table-layout', 'fixed');
        $gridBody.css({ right: 0, top: headHeight, width: 'auto' });
        setTimeout(adjustGrid);
    }

    function adjustGrid() {
        var $table = $gridBody.children('table');
        var $cell = $table.children('tbody')
                        .children('tr:first-child')
                        .children('td:last-child');
        var tableWidth = $table.width()-$cell.width();
        $gridBody.css({
            top: $gridHead.height(),
            'overflow-y': 'auto',
            'overflow-x': (tableWidth < $gridBody.width()-environ.scrollbarWidth) ?
                            'hidden' : 'auto'
        });
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

    $('#schema').hide();
    $('#help').hide();
    $('#close-sql').hide();

    $($database).text(config.databaseName);
    updateTitle();

    if (config.evaluateOnStart) {
        $('#run').click();
    }

});

