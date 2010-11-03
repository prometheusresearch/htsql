var prefix = HTRAF_PREFIX || '';
if(!window.$) {
    document.write('<script type="text/javascript" '
        + ' src="' + prefix + '/js/jquery-1.4.3.min.js"></script>\n');

    // jsPlot
    document.write('<!--[if IE]><script type="text/javascript" '
        + ' src="' + prefix + '/js/excanvas.min.js"></script><![endif]-->\n');
    document.write('<script type="text/javascript" '
        + ' src="' + prefix + '/js/jquery.jqplot.min.js"></script>\n');
    document.write('<script type="text/javascript" '
        + ' src="' + prefix + '/js/jqplot.pieRenderer.min.js"></script>\n');
    document.write('<link rel="stylesheet" type="text/css"'
                   + 'href="' + prefix + '/css/jquery.jqplot.min.css">');
}

if(!window.htraf) {
    document.write('<script type="text/javascript" '
        + 'src="' + prefix + '/js/_htraf.js"></script>\n');
    document.write('<link rel="stylesheet" type="text/css"'
                   + 'href="' + prefix + '/css/htraf.css">');
}
