

$(function () {
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


