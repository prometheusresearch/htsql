if(!window.$) {
    document.write('<script type="text/javascript" '
        + ' src="/external/jquery/jquery-1.4.3.min.js"></script>\n');

    // jsPlot
    document.write('<!--[if IE]><script type="text/javascript" '
        + ' src="/external/jqplot/excanvas.min.js"></script><![endif]-->\n');
    document.write('<script type="text/javascript" '
        + ' src="/external/jqplot/jquery.jqplot.min.js"></script>\n');
    document.write('<script type="text/javascript" '
        + ' src="/external/jqplot/plugins/jqplot.pieRenderer.min.js"></script>\n');
    document.write('<link rel="stylesheet" type="text/css"'
        + 'href="/external/jqplot/jquery.jqplot.min.css">');
}

if(!window.htraf) {
    document.write('<script type="text/javascript" '
        + 'src="/htraf/js/_htraf.js"></script>\n');
    document.write('<link rel="stylesheet" type="text/css"'
        + 'href="/htraf/css/htraf.css">');
}
