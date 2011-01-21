

$(function () {
    $('.htsql-input pre').prepend("<div class=\"htsql-toggle\">[+]</div>");
    $('.htsql-toggle').click(function () {
        var output = this;
        if ($(this).text() == '[+]') {
            $(this).parents('.htsql-input').next('.htsql-output').slideDown('fast');
            $(this).text('[-]');
        }
        else {
            $(this).parents('.htsql-input').next('.htsql-output').slideUp('fast');
            $(this).text('[+]');
        }
    });
});


