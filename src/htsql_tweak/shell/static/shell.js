
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
            evaluateOnStart: ($body.attr('evaluate-on-start') == 'true')
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
            table: null,
        }
    }

    function makeEditor() {
        var $editor = $('#editor');
        var editor = CodeMirror.fromTextArea($editor[0], {
            mode: 'htsql',
            onKeyEvent: function(i, e) {
                if (e.ctrlKey && e.keyCode == 13) {
                    if (e.type == 'keyup') {
                        $('#run').click();
                    }
                    return true;
                }
            }
        });
        return editor;
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

    function clickClose() {
        if (state.$panel) {
            state.$panel.hide();
            state.$panel = null;
        }
    }

    function scrollGrid() {
        $gridHead.scrollLeft($gridBody.scrollLeft());
    }

    function run(query) {
        if (!query)
            return;
        if (!config.serverRoot)
            return;
        if (state.waiting)
            return;
        state.lastQuery = query;
        query = "/evaluate('"+query.replace(/'/g, "''")+"')";
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
        $error.html(output.message);
    }

    function handleUnsupported(output) {
        if (state.$panel)
            state.$panel.hide();
        state.$panel = null;
        var url = config.serverRoot+escape(state.lastQuery);
        window.open(url, "_blank");
    }

    function handleEmpty(output) {
        if (state.$panel)
            state.$panel.hide();
        state.$panel = null;
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
    var $popups = $('#popups');
    var $morePopup = $('#more-popup');

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
    $('#close-error').click(clickClose);
    $('#close-failure').click(clickClose);
    $('#grid-body').scroll(scrollGrid);
    $('#popups').click(clickPopups);

    $('#schema').hide();
    $('#help').hide();

    $('title').text(config.databaseName);
    $('database').text(config.databaseName);

    if (config.evaluateOnStart) {
        $('#run').click();
    }

});

