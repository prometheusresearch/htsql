
$(document).ready(function() {

    function get_scrollbar_size() {
        var outer_div = $("<div></div>").prependTo($('.viewport'))
                        .css({ width: 100, height: 100, overflow: 'auto',
                               position: 'absolute', left: 0, top: -1000 });
        var inner_div = $("<div></div>").prependTo(outer_div)
                        .css({ width: '100%', height: 1000 });
        var scrollbar_size = 100-inner_div.width()+1;
        outer_div.remove();
        return scrollbar_size;
    }

    function make_editor() {
        var editor = $('#editor');
        var cm = CodeMirror.fromTextArea(editor[0], {
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
        return cm;
    }

    var global_start = 0;
    function handle_run() {
        var query = editor.getValue();
        query = query.replace(/^\s+|\s+$/, "");
        if (!/^\//.test(query)) {
            return;
        }
        query = "/evaluate('"+escape(query.replace(/'/g, "''"))+"')";
        var uri = base_uri+escape(query);
        $.getJSON(uri, handle_result);
        global_start = Date.now();
    }

    var handle_result_start = 0;
    var handle_result_end = 0;

    function handle_result(output) {
        handle_result_start = Date.now();
        var style = output.style;
        var head = output.head;
        var body = output.body;
        var size = style.length;
        table_data = '';
        table_data += '<table>';
        table_data += '<thead>';
        for (var k = 0; k < head.length; k ++) {
            var row = head[k];
            table_data += '<tr>';
            table_data += '<th class="dummy">&nbsp;</th>';
            for (var i = 0; i < head[k].length; i ++) {
                var cell = row[i];
                var title = cell[0];
                var colspan = cell[1];
                var rowspan = cell[2];
                table_data += '<th' + (colspan>1 ? ' colspan="'+colspan+'"' : '')
                                    + (rowspan>1 ? ' rowspan="'+rowspan+'"' : '') + '>'
                              + '<span>' + title + '</span></th>';
            }
            if (k == 0) {
                table_data += '<th' + (head.length > 1 ? ' rowspan="'+head.length+'"' : '')
                              + ' class="dummy">&nbsp;</th>';
            }
            table_data += '</tr>';
        }
        table_data += '</thead>';
        table_data += '<tbody>';
        for (var k = 0; k < body.length; k ++) {
            var row = body[k];
            table_data += '<tr' + (k % 2 == 1 ? ' class="alt">' : '>');
            table_data += '<th><span>'+(k+1)+'</span></th>';
            for (var i = 0; i < size; i ++) {
                var value = row[i];
                if (value == null) {
                    value = '&nbsp;'
                }
                table_data += '<td><span' + (style[i] ? ' class="'+style[i]+'">' : '>')
                              + value + '</span></td>';
            }
            table_data += '<td class="dummy">&nbsp;</td>';
            table_data += '</tr>';
        }
        table_data += '<tr>';
        table_data += '<th class="dummy">&nbsp;</th>';
        for (var i = 0; i < size; i ++) {
            table_data += '<td class="dummy">&nbsp;</td>';
        }
        table_data += '<td class="dummy">&nbsp;</td>';
        table_data += '</tbody>';
        table_data += '</table>';
        setTimeout(reset_grid, 0);
        handle_result_end = Date.now();
    }

    function handle_resize() {
        setTimeout(reset_grid, 0);
    }

    function handle_scroll() {
        grid_head.scrollLeft(grid_body.scrollLeft());
    }

    var reset_grid_start = 0;
    var reset_grid_end = 0;
    function reset_grid() {
        reset_grid_start = Date.now();
        grid.removeAttr('style');
        grid_head.removeAttr('style').empty();
        grid_body.removeAttr('style').empty();
        if (table_data) {
            var height = viewport.height() - grid.offset().top;
            height = Math.floor(height);
            height = height > 0 ? height : 0;
            grid_body.height(height);
            grid_body.html(table_data);
            setTimeout(split_table, 0);
        }
        reset_grid_end = Date.now();
    }

    var split_table_start = 0;
    var split_table_end = 0;
    function split_table() {
        split_table_start = Date.now();
        var col_widths = [];
        grid_body.find('tbody').find('tr:first-child').children().each(function(idx) {
            col_widths[idx] = $(this).width()+2;
        });
        grid_body.height(grid_body.height()-grid_body.find('thead').height());
        $("<table></table>").appendTo(grid_head).append(grid_body.find('thead'));
        var total_width = 0;
        for (var idx = col_widths.length-1; idx >= 0; idx --) {
            total_width += col_widths[idx];
        }
        var grid_width = grid_body.width();
        if (total_width < grid_width-scrollbar_size-1) {
            grid_body.css('overflow-x', 'hidden');
        }
        if (total_width < grid_width) {
            var diff = grid_width-total_width;
            col_widths[col_widths.length-1] += diff;
            total_width += diff;
        }
        var head_table = grid_head.children('table');
        var body_table = grid_body.children('table');
        for (var idx = col_widths.length-1; idx >= 0; idx --) {
            var width = col_widths[idx];
            body_table.prepend("<col style=\"width: "+width+"px\">");
            if (idx == col_widths.length-1) {
                width += 100;
            }
            head_table.prepend("<col style=\"width: "+width+"px\">");
        }
        body_table.width(total_width)
            .css('table-layout', 'fixed').scroll(handle_scroll);
        head_table.width(total_width+100)
            .css('table-layout', 'fixed');
        setTimeout(report, 0);
        split_table_end = Date.now()
    }

    var global_end = 0;
    function report() {
        global_end = Date.now();
        console.log("---");
        console.log("delay: "+(handle_result_start-global_start)/1000);
        console.log("handle_result: "+(handle_result_end-handle_result_start)/1000);
        console.log("delay: "+(reset_grid_start-handle_result_end)/1000);
        console.log("reset_grid: "+(reset_grid_end-reset_grid_start)/1000);
        console.log("delay: "+(split_table_start-reset_grid_end)/1000);
        console.log("split_table: "+(split_table_end-split_table_start)/1000);
        console.log("delay: "+(global_end-split_table_end)/1000);
        console.log("total: "+(global_end-global_start)/1000);
    }

    /*var base_uri = "http://demo.htsql.org";*/
    var base_uri = "http://localhost:8080";
    var table_data = null;
    var viewport = $('.viewport');
    var grid = $('.grid');
    var grid_head = $('.grid-head');
    var grid_body = $('.grid-body');
    var editor = make_editor();
    editor.setCursor(0, 1);
    editor.focus();
    var scrollbar_size = get_scrollbar_size();

    $('#run').click(handle_run);
    grid_body.scroll(handle_scroll);
    $(window).resize(handle_resize);
});

