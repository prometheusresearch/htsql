(function($) {

if(window.htraf)
    return;

if(!Array.prototype.map) {
    Array.prototype.map = function(func) {
        var ret = [];
        for(var i = 0, l = this.length; i < l; i++)
            ret.push(func(this[i]));
        return ret;
    }
}

htraf = {};

htraf.quote = function(s) {
    return "'" + s.replace(/'/g, "''") + "'";
};

htraf.convertNull = function(value) {
    return value === null ? '':value;
}

htraf.iterVars = function(s, callback) {
    var reStr = /^'[^']*'/, reNonStr = /^[^']+('|$)/, reVar = /\$\w+/g,
        ret = '';
    while(s) {
        var match = s.match(reStr);
        if(match) {
            match = match[0];
            ret += match;
            s = s.substr(match.length, s.length);
            continue;
        }
        var match = s.match(reNonStr);
        if(match) {
            match = match[0];
            if(match[match.length - 1] == "'")
                match = match.substr(0, match.length - 1);
            var vars = match.match(reVar) || [];
            ret += (callback(match, vars) || match);
            s = s.substr(match.length, s.length);
            continue;
        }

        // nothing matched, most likely wrong HTSQL
        return ret + s;
    }
    return ret;
};

htraf.getVars = function(s) {
    var varDict = {};
    htraf.iterVars(s, function(fragment, vars) {
        vars.map(function(v) {
            varDict[v.substr(1, v.length)] = true;    
        })
    });
    
    var ret = [];
    for(var v in varDict)
        ret.push(v);
    return ret;
};

htraf.subVars = function(s, vars) {
    for(var key in vars) 
        vars[key] = htraf.quote(vars[key]);
    return htraf.iterVars(s, function(fragment) {
        for(var key in vars) {
            var re = eval('(/\\$' + key + '/)')
            fragment = fragment.replace(re, vars[key]);
        }
        return fragment;
    });
};

htraf.load = function(url, success) {
    if(url[0] != '/')
        url = '/' + url;
    $.ajax({
        type: 'GET',
        url: htraf.prefix + url + '/:json',
        cache: true,
        dataType: 'json',
        success: function(data) {
            success(data);
        }
    });
};

htraf.init = function(prefix) {
    if(prefix[prefix.length - 1] == '/')
        prefix = prefix.substr(0, prefix.length - 1);
    htraf.prefix = prefix || '';
    htraf.widgets.render();
    htraf.widgets.configureLinked();
};

htraf.widgets = {
    list: [
        {
            selector: 'table[data-source]',
            render: function(el) {
                $(el).htraf({
                    getValue: function() {
                        return $(this).find('tr.row-selected td:eq(0)').text()
                               || null;
                    },
                    
                    render: function(data) {
                        var tr = $('<tr/>').appendTo(this), self = this;
                        data[0].map(function(text) {
                            $('<th/>').text(text).appendTo(tr);
                        });
                        data.slice(1, data.length).map(function(row) {
                            var tr = $('<tr/>')
                                .mouseover(function() {
                                    $(this).addClass('row-hover');
                                }) 
                                .mouseout(function() {
                                    $(this).removeClass('row-hover');
                                })
                                .click(function() {
                                    $('tr', self).removeClass('row-selected');
                                    $(this).addClass('row-selected');
                                    $(self).trigger('change');
                                }).appendTo(self);
                            row.map(function(text) {
                                $('<td/>').text(htraf.convertNull(text))
                                          .appendTo(tr);
                            });
                        });

                        var hide = ($(this).attr('data-hide-first-column') 
                                    || '').toLowerCase();
                        if(hide == 'yes' || hide == 'true') {
                            $('tr', this).find('td:eq(0),th:eq(0)').hide();
                        }
                    }
                }) 
            }
        },

        {
            selector: 'select[data-source]',
            render: function(el) {
                $(el).htraf({
                    render: function(data) {
                        var titleIndex = data[0].length > 1 ? 1:0;
                        for(var i = 1, l = data.length; i < l; i++) 
                            this.options[i - 1] = 
                            new Option(htraf.convertNull(data[i][titleIndex]),
                                       htraf.convertNull(data[i][0]));
                        $(this).trigger('change');
                    }        
                });
            }
        },

        {
            selector: 'div[data-source][data-display=chart]',
            render: function(el) {
                
            }
        },

        {
            selector: 'input[data-source],textarea[data-source]',
            render: function(el) {
                $(el).htraf({
                    render: function(data) {
                        var text = (data[1] || [''])[0] || '';
                        $(this).val(htraf.convertNull(text));
                    }
                });
            }
        },

        {
            selector: '[data-source]',
            render: function(el) {
                $(el).htraf({
                    render: function(data) {
                        var text = (data[1] || [''])[0] || '';
                        $(this).text(htraf.convertNull(text));
                    },

                    getValue: function() {
                        return $(this).text() || null;
                    }
                });
            
            }
        }
    ],

    registry: $(), 

    render: function() {
        this.list.map(function(widget) {
            $(widget.selector).each(function() {
                if($(this).data('htraf'))
                    return;
                widget.render(this);
                var self = this;
                $(this).htraf('getLinked').change(function() {
                    var value = $(this).htraf('getValue') || $(this).val();
                    if(typeof value == 'undefined' || value == null)
                        $(self).htraf('clear');
                    else {
                        $(self).htraf('clear').htraf('load');
                    }
                });
            });
        });    
    },

    configureLinked: function() {
        var linked = $();
        htraf.widgets.registry.each(function() {
            linked.add($(this).htraf('getLinked'));
        });

        // configure buttons, so that they produce 'change' event on click
        linked.filter('button,input[type=button]').click(function() {
            $(this).trigger('change');
        });


        // load all those which don't have dependencies
        htraf.widgets.registry.add(linked).filter(function() {
            return $(this).htraf('getLinked').size() == 0;        
        }).htraf('load');
    } 
};

$.fn.extend({
    htraf: function(opts) {
        var args = $.makeArray(arguments);
        args = args.slice(1, args.length);

        if(typeof opts == 'string' && opts.substr(0, 3) == 'get') {
            var obj = this.size() && $(this[0]).data('htraf') || null;
            return obj ? (obj[opts] && obj[opts].apply(this[0], args)):null;
        }

        return this.each(function() {
            var obj = $(this).data('htraf') || null;
            if(typeof opts == 'string') {
                return obj ? (obj[opts] && obj[opts].apply(this, args)):null;
            }
            if(obj)
                return null;
            
            htraf.widgets.registry = htraf.widgets.registry.add($(this));

            opts = opts || {};
            $(this).data('htraf', {
                opts: opts.opts || {},
                load: function() {
                    var url = $(this).attr('data-source') || null;
                    if(!url)
                        return;
                    
                    var vars = {};
                    $(this).htraf('getLinked').each(function() {
                        var id = $(this).attr('id'),
                            value = $(this).htraf('getValue') || $(this).val();
                        if(!id || value === null)
                            return;
                        vars[id] = value;
                    });
                    url = htraf.subVars(url, vars);

                    var self = this;
                    htraf.load(url, function(data) {
                        $(self).htraf('render', data);    
                    });
                },

                clear: opts.clear || function() {
                    $(this).text('');
                    $(this).children().remove();
                    $(this).trigger('change');
                }, 

                render: opts.render || function(data) {
                
                },

                getValue: opts.getValue || function() {
                    return $(this).val();        
                },

                getLinked: function() {
                    var ret = $(), selectorAttr = $(this).attr('data-linked');
                        selectorVars = 
                            htraf.getVars($(this).attr('data-source'))
                            .map(function(v) {
                                return '#' + v;   
                            }).join(',');
                    if(selectorAttr)
                        ret = ret.add(selectorAttr);
                    if(selectorVars)
                        ret = ret.add(selectorVars);
                    return ret;
                }
            });
        });
    } 
});

$(function() {
    htraf.init(HTSQL_PREFIX);
});

})(jQuery);
