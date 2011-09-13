
$(document).ready(function() {

    function handle_resize() {
        setTimeout(resize_grid, 0);
    }

    function handle_scroll() {
        $('.grid-head').scrollLeft($('.grid-body').scrollLeft());
    }

    function resize_grid() {
        var height = $('.export-buttons').offset().top - $('.grid-body').offset().top;
        height = Math.floor(height);
        height = height > 0 ? height : 0;
        $('.grid-body').height(height);
        $('.grid').css('visibility', 'visible');
    }

    function split_table() {
        var col_widths = [];
        $('.grid tbody tr:first-child').children().each(function(idx) {
            col_widths[idx] = $(this).width()+2;
        });
        $('.grid-head').append("<table></table>");
        $('.grid-head table').append($('.grid-body thead'));
        var total_width = 0;
        for (var idx = col_widths.length-1; idx >= 0; idx --) {
            var width = col_widths[idx];
            total_width += width;
            $('.grid-head table').prepend("<col style=\"width: "+width+"px\">");
            $('.grid-body table').prepend("<col style=\"width: "+width+"px\">");
        }
        $('.grid-head .expander').css("left", ""+total_width+100+"px");
        $('.grid-head table').width(total_width);
        $('.grid-body table').width(total_width);
        $('.grid-head table').css('table-layout', 'fixed');
        $('.grid-body table').css('table-layout', 'fixed');
        $('.grid-body').scroll(handle_scroll);
        setTimeout(resize_grid, 0);
    }

    var editor = $('#editor');
    var cm = CodeMirror.fromTextArea(editor[0], {
        mode: 'htsql'
    });
    $('.grid').css('visibility', 'hidden');
    setTimeout(split_table, 0);
    $(window).resize(handle_resize);
});

