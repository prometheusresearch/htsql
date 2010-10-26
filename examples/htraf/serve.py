
import sys, os
from paste.httpserver import serve
from paste.urlmap import URLMap
from paste.fileapp import DirectoryApp
from htsql.application import Application as Htsql

if __name__ == '__main__':
    assert len(sys.argv) > 1, ("At least 1 command line parameter is required"
                               "(database URL)")
    db = sys.argv[1]
    host = sys.argv[2] if len(sys.argv) > 2 else 'localhost'
    port = sys.argv[3] if len(sys.argv) > 3 else '8080'
    file = os.path.abspath(os.path.join(os.getcwd(), __file__))
    dirname = os.path.join(os.path.dirname(file), 'static')
    app = URLMap()
    app['/'] = DirectoryApp(dirname)
    app['/@htsql_regress'] = Htsql(db)
    serve(app, host=host, port=port)
