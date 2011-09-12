
$(document).ready(function() {

    function handle_resize() {
        var output_height = $(window).height()
                            -$('.heading-area').outerHeight()
                            -$('.input-area').outerHeight()
                            -$('.footer-area').outerHeight();
        if (output_height < 240) {
            output_height = 240;
        }
        $('.output-area').height(output_height);
        var wrapper_height = output_height - $('.export-buttons').outerHeight();
        $('.grid-wrapper').height(wrapper_height);
        if ($('#grid').css('visibility') == 'hidden') {
            setTimeout(split_table, 0);
        }
    }

    function split_table() {
        /*
        $('#values td, #values th').each(function() {
            $(this).width($(this).width());
            $(this).height($(this).height());
        });
        */
        $('#values thead th').each(function() {
            $(this).width($(this).width());
        });
        $('#values tbody tr:first-child td').each(function() {
            $(this).width($(this).width());
        });
        $('#values tbody tr').each(function() {
            $(this).height($(this).height());
        });
        $('#values table').clone().appendTo('#titles .scroll');
        $('#values table').clone().appendTo('#indices .scroll');
        $('#titles tbody').remove();
        $('#titles tr:first-child th:first-child').remove();
        $('#indices thead').remove();
        $('#indices td').remove();
        $('#values thead').remove();
        $('#values th').remove();
        setTimeout(resize_grid, 0);
    }

    function resize_grid() {
        var height = $('.export-buttons').offset().top - $('#values .scroll').offset().top;
        if (height < $('#values .scroll').height()) {
            height = Math.floor(height);
            $('#values .scroll').height(height);
            $('#indices .scroll').height(height);
            $('#values .scroll').scroll(handle_vertical_scroll);
        }
        setTimeout(show_grid, 0);
    }

    function show_grid() {
        $('#grid').css('visibility', 'visible');
    }

    function handle_vertical_scroll() {
        $('#indices .scroll').scrollTop($('#values .scroll').scrollTop());
    }

    var editor = $('#editor');
    var cm = CodeMirror.fromTextArea(editor[0], {
        mode: 'htsql'
    });
    $('#grid').css('visibility', 'hidden');
    setTimeout(handle_resize, 0);
    $(window).resize(handle_resize);
});

