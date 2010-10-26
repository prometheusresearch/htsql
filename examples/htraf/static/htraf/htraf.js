var prefix = HTRAF_PREFIX || '/';
if(!window.$) {
    document.write('<script type="text/javascript" '
        + ' src="' + prefix + 'js/jquery-1.4.3.min.js"></script>\n');
}

if(!window.htraf) {
    document.write('<script type="text/javascript" '
        + 'src="' + prefix + 'js/_htraf.js"></script>\n');
}
