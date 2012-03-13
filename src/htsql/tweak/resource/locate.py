#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.context import context
from ...core.util import maybe
from ...core.adapter import Protocol
import mimetypes
import hashlib
import os, os.path
import cStringIO
import zipfile
import urllib2
import pkg_resources


class Resource(object):

    def __init__(self, name, data, mimetype=None, disposition='inline',
                 etag=None):
        assert isinstance(name, str) and '/' not in name
        assert isinstance(data, str)
        assert isinstance(mimetype, maybe(str))
        assert disposition in ['inline', 'attachment']
        assert isinstance(etag, maybe(str))
        if mimetype is None:
            mimetype = (mimetypes.guess_type(name)[0] or
                        'application/octet-stream')
        if etag is None:
            etag = hashlib.md5(data).hexdigest()
        self.name = name
        self.data = data
        self.mimetype = mimetype
        self.disposition = disposition
        self.etag = etag


class Locate(Protocol):

    @classmethod
    def __matches__(component, dispatch_key):
        return any(dispatch_key.startswith(name)
                   for name in component.__names__)

    @classmethod
    def __dominates__(component, other):
        if issubclass(component, other):
            return True
        for name in component.__names__:
            for other_name in other.__names__:
                if name.startswith(other_name) and name != other_name:
                    return True
        return False

    def __init__(self, path):
        assert isinstance(path, str) and path.startswith('/')
        self.path = path
        self.prefix = None
        self.suffix = None
        for name in self.__names__:
            if path.startswith(name):
                self.prefix = name
                self.suffix = path[len(name):]

    def __call__(self):
        return None


class LocatePackage(Locate):

    package = None
    directory = None

    def __call__(self):
        filename = pkg_resources.resource_filename(self.package,
                            os.path.join(self.directory, self.suffix))
        if not os.path.isfile(filename):
            return super(LocatePackage, self).__call__()
        name = os.path.basename(filename)
        stream = open(filename)
        data = stream.read()
        stream.close()
        stat = os.stat(filename)
        etag = "%s-%s" % (int(stat.st_mtime), stat.st_size)
        return Resource(name, data, etag=etag)


class LocateRemote(Locate):

    url = None
    md5 = None
    cache = None
    is_zip = False

    def __init__(self, path):
        super(LocateRemote, self).__init__(path)
        self.init_repository()

    def init_repository(self):
        userdir = os.path.expanduser('~')
        staticdir = os.path.join(userdir, '.htsql')
        resourcedir = os.path.join(staticdir, 'resource')
        cachedir = os.path.join(resourcedir, '%s.%s' % (self.cache, self.md5))
        if not os.path.exists(cachedir):
            addon = context.app.tweak.resource
            with addon.lock:
                if not os.path.exists(staticdir):
                    os.mkdir(staticdir, 0700)
                if not os.path.exists(resourcedir):
                    os.mkdir(resourcedir)
                if not os.path.exists(cachedir):
                    self.init_cache(cachedir)
        self.repository = cachedir

    def init_cache(self, cachedir):
        stream = urllib2.urlopen(self.url)
        data = stream.read()
        stream.close()
        assert hashlib.md5(data).hexdigest() == self.md5
        filename = os.path.join(cachedir, os.path.basename(self.url))
        if not self.is_zip:
            os.mkdir(cachedir)
            stream = open(filename, 'wb')
            stream.write(data)
            stream.close()
        else:
            self.unpack(cachedir, data)

    def unpack(self, cachedir, data):
        archive = zipfile.ZipFile(cStringIO.StringIO(data))
        entries = archive.infolist()
        if not entries:
            return
        common = entries[0].filename
        if not (common.endswith('/') and
                all(entry.filename.startswith(common)
                    for entry in entries)):
            common = ''
        os.mkdir(cachedir)
        for entry in entries:
            filename = entry.filename[len(common):]
            if filename.startswith('/'):
                filename = filename[1:]
            if not filename:
                continue
            filename = os.path.join(cachedir, filename)
            if filename.endswith('/'):
                os.mkdir(filename)
            else:
                stream = open(filename, 'wb')
                stream.write(archive.read(entry))
                stream.close()

    def __call__(self):
        filename = os.path.join(self.repository, self.suffix)
        if not os.path.isfile(filename):
            return super(LocateRemote, self).__call__()
        name = os.path.basename(filename)
        stream = open(filename)
        data = stream.read()
        stream.close()
        stat = os.stat(filename)
        etag = "%s-%s" % (int(stat.st_mtime), stat.st_size)
        return Resource(name, data, etag=etag)


locate = Locate.__invoke__


