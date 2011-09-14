
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
            mode: 'htsql'
        });
        return cm;
    }

    function handle_resize() {
        setTimeout(resize_grid, 0);
    }

    function handle_scroll() {
        grid_head.scrollLeft(grid_body.scrollLeft());
    }

    function resize_grid() {
        var height = viewport.height() - grid_body.offset().top;
        height = Math.floor(height);
        height = height > 0 ? height : 0;
        grid_body.height(height);
        grid.css('visibility', 'visible');
    }

    function split_table() {
        var col_widths = [];
        grid_body.find('tbody tr:first-child').children().each(function(idx) {
            col_widths[idx] = $(this).width()+2;
        });
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
        body_table.width(total_width).css('table-layout', 'fixed').scroll(handle_scroll);
        head_table.width(total_width+100).css('table-layout', 'fixed');
        grid_body.scroll(handle_scroll);
        setTimeout(resize_grid, 0);
    }

    var viewport = $('.viewport');
    var grid = $('.grid');
    var grid_head = $('.grid-head');
    var grid_body = $('.grid-body');
    var editor = make_editor();
    var scrollbar_size = get_scrollbar_size();

    $('.grid').css('visibility', 'hidden');
    setTimeout(split_table, 0);
    $(window).resize(handle_resize);
});

