

function initToggles() {
    $('.htsql-input pre').prepend("<div class=\"htsql-toggle\">[+]</div>");
    $('.htsql-toggle').click(function () {
        var output = this;
        if ($(this).text() == '[+]') {
            $(this).parents('.htsql-input').next('.htsql-output').css('display', 'block');
            $(this).text('[-]');
        }
        else {
            $(this).parents('.htsql-input').next('.htsql-output').css('display', 'none');
            $(this).text('[+]');
        }
    });
}


$(initToggles);


