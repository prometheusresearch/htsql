#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from job import job, log, debug, fatal, warn, run, ls, cp, rmtree, mktree, pipe
from vm import LinuxBenchVM, WindowsBenchVM, DATA_ROOT, CTL_DIR
from dist import pipe_python, setup_py
import os, re, StringIO, pprint
import setuptools
import yaml


KEYSIG = '8E70D862' # identifier for HTSQL package signing key
src_vm = LinuxBenchVM('src', 'debian', 22)
rpm_vm = LinuxBenchVM('rpm', 'centos', 22)
deb_vm = LinuxBenchVM('deb', 'debian', 22)


class Move(object):

    def __init__(self, code, files, variables):
        assert isinstance(code, str)
        assert isinstance(files, dict)
        for key in sorted(files):
            assert isinstance(key, str)
        assert isinstance(variables, dict)
        for key in sorted(variables):
            assert isinstance(key, str)
        self.code = code
        self.files = files
        self.variables = variables

    def __call__(self, src, dst):
        assert os.path.isdir(src)
        assert os.path.isdir(dst)
        for filename in sorted(self.files):
            parts = self.files[filename]
            if parts is None:
                parts = []
            elif isinstance(parts, str):
                parts = [parts]
            assert isinstance(parts, list) and all(isinstance(part, str)
                                                   for part in parts)
            if not parts:
                continue
            filename = os.path.join(dst, filename)
            dirname = os.path.dirname(filename)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            stream = open(filename, 'w')
            for part in parts:
                if not part:
                    stream.write("\n")
                    continue
                part = os.path.join(src, part)
                assert os.path.isfile(part)
                part = open(part).read()
                part = self.substitute(part)
                stream.write(part)

    def substitute(self, data):
        def replace(match):
            key = match.group('key')
            key = key.lower().replace('_', '-')
            assert key in self.variables, key
            value = self.variables[key]
            assert value is None or isinstance(value, str), key
            if value is None:
                value = ""
            return value
        return re.sub(r"(?<!\$)\$\{(?P<key>[0-9a-zA-Z_-]+)\}", replace, data)

    @classmethod
    def load(cls, filename):
        common_files = {}
        common_variables = {}
        moves = []
        for document in yaml.load_all(open(filename)):
            assert isinstance(document, dict)
            assert all(key in ['code', 'files', 'variables']
                       for key in sorted(document))
            code = document.get('code')
            files = document.get('files', {})
            variables = document.get('variables', {})
            if code is None:
                common_files.update(files)
                common_variables.update(variables)
                continue
            for key in common_files:
                files.setdefault(key, common_files[key])
            for key in common_variables:
                variables.setdefault(key, common_variables[key])
            move = cls(code, files, variables)
            moves.append(move)
        return moves

load_moves = Move.load


@job
def pkg_src():
    """create a source package

    This job creates Python source distribution.
    """
    if src_vm.missing():
        raise fatal("VM is not built: %s" % src_vm.name)
    if src_vm.running():
        src_vm.stop()
    if os.path.exists("./build/pkg/src"):
        rmtree("./build/pkg/src")
    if os.path.exists("./build/tmp"):
        rmtree("./build/tmp")
    version = yaml.load(pipe_python("-c 'import setup, yaml;"
                                    " print yaml.dump(setup.get_version())'"))
    all_addons = yaml.load(pipe_python("-c 'import setup, yaml;"
                                       " print yaml.dump(setup.get_addons())'"))
    moves = load_moves(DATA_ROOT+"/pkg/source/moves.yaml")
    src_vm.start()
    try:
        for move in moves:
            with_doc = move.variables['with-doc']
            packages = move.variables['packages'].strip().splitlines()
            addons = "".join(addon+"\n" for addon in all_addons
                             if addon.split('=', 1)[1].strip().split('.')[0]
                                                                in packages)
            move.variables['version'] = version
            move.variables['htsql-addons'] = addons
            mktree("./build/tmp")
            run("hg archive ./build/tmp/htsql")
            for dirname in ls("./build/tmp/htsql/src/*"):
                if os.path.basename(dirname) not in packages:
                    rmtree(dirname)
            packages = setuptools.find_packages("./build/tmp/htsql/src")
            move.variables['packages'] = "".join(package+"\n"
                                                 for package in packages)
            if not with_doc:
                rmtree("./build/tmp/htsql/doc")
            move(DATA_ROOT+"/pkg/source", "./build/tmp/htsql")
            src_vm.put("./build/tmp/htsql", ".")
            if with_doc:
                src_vm.run("cd htsql &&"
                           " PYTHONPATH=src sphinx-build -d doc doc doc/html")
                for filename in ls("./build/tmp/htsql/doc/man/*.?.rst"):
                    basename = os.path.basename(filename)
                    target = basename[:-4]
                    src_vm.run("rst2man htsql/doc/man/%s htsql/doc/man/%s"
                               % (basename, target))
            src_vm.run("cd htsql && python setup.py sdist --formats=zip,gztar")
            if not os.path.exists("./build/pkg/src"):
                mktree("./build/pkg/src")
            src_vm.get("./htsql/dist/*", "./build/pkg/src")
            src_vm.run("rm -rf htsql")
            rmtree("./build/tmp")
    finally:
        src_vm.stop()
    log()
    log("The generated source packages are placed in:")
    for filename in ls("./build/pkg/src/*"):
        log("  `%s`" % filename)
    log()


class Packager(object):

    def __init__(self, vm):
        self.vm = vm
        if self.vm.missing():
             raise fatal("VM is not built: %s" % self.vm.name)
        source_paths = ls("./build/pkg/src/HTSQL-*.tar.gz")
        if not source_paths:
            raise fatal("cannot find a source package; run `job pkg-src` first")
        if len(source_paths) > 1:
            raise fatal("too many source packages in `./build/pkg/src`")
        [self.source_path] = source_paths
        self.archive = os.path.basename(self.source_path)
        self.upstream_version = re.match(r'^HTSQL-(?P<version>.+).tar.gz$',
                                         self.archive).group('version')
        (self.version, self.suffix) = re.match(r'^(\d+\.\d+.\d+)(.*)$', 
                                               self.upstream_version).groups()
        self.build_path = "build/pkg/%s" % self.vm.name

    def __call__(self):
        if self.vm.running():
            self.vm.stop()
        if os.path.exists(self.build_path):
            rmtree(self.build_path)
        self.vm.start()
        try:
            # install signing key
            pubkey = pipe("gpg --armour --export %s" % KEYSIG)
            seckey = pipe("gpg --armour --export-secret-key %s" % KEYSIG)
            self.vm.write("/root/sign.key", pubkey + seckey)
            self.vm.run("gpg --import /root/sign.key")
            # upload package source, build and test
            self.vm.put(self.source_path, '/root')
            mktree(self.build_path)
            self.package()
            self.vm.run("htsql-ctl get sqlite:test.db /%27Hello%20World%27")
            self.vm.get(self.build_file, self.build_path)
        finally:
            self.vm.stop()
        log()
        log("Package is built successfully:")
        log("  %s/`%s`" % (self.build_path, self.build_file))
        log()


class DEB_Packager(Packager):

    def __init__(self):
        Packager.__init__(self, deb_vm)
        # create debian_version variable
        if self.suffix:
            version = self.version + "~" + self.suffix
        else:
            version = self.version
        changelog = open(DATA_ROOT+"/pkg/debian/changelog").read()
        if ('htsql (%s-1)' % version) not in changelog:
            raise fatal("update debian/changelog for %s release" % version)
        self.debian_version = version
        self.build_file = "htsql_%s-1_all.deb" % version
        
    def package(self):
        self.vm.run("mv %s htsql_%s.orig.tar.gz" % (self.archive, self.debian_version))
        self.vm.run("tar xvfz htsql_%s.orig.tar.gz" % self.debian_version)
        self.vm.put(DATA_ROOT+"/pkg/debian", "HTSQL-%s" % self.upstream_version)
        self.vm.run("cd HTSQL-%s && dpkg-buildpackage -k%s" % \
                     (self.upstream_version, KEYSIG))
        self.vm.run("dpkg -i %s" % self.build_file)

               
@job
def pkg_deb():
    """create a debian package

    This job creates combined source & binary package.

    You must also append a release note to tool/data/pkg/debian/changelog
    that matches the current release version.  Debian's pre-release naming
    convention is different than Python's since it requires a tilde 
    before the b1, rc1, etc.
    """
    packager = DEB_Packager()
    packager()


class RPM_Packager(Packager):

    def __init__(self):
        Packager.__init__(self, rpm_vm)
        self.distribution = 'el6'
        self.build_file = "HTSQL-%s-1.%s.noarch.rpm" % \
                             (self.upstream_version, self.distribution)
        
    def package(self):
        self.vm.run("mkdir -p rpmbuild/{BUILD,RPMS,S{OURCE,PEC,RPM}S}")
        self.vm.put(DATA_ROOT+"/pkg/redhat", "redhat")
        self.vm.run("cp redhat/.rpmmacros ~ ")
        self.vm.run("mv redhat/HTSQL.spec rpmbuild/SPECS")
        self.vm.run("mv %s rpmbuild/SOURCES" % self.archive)
        self.vm.run("rpmbuild -bb rpmbuild/SPECS/HTSQL.spec")
        self.vm.run("mv rpmbuild/RPMS/noarch/%s ~" % self.build_file)
        #self.vm.run("rpmsign --addsign %s" % self.build_file)
        self.vm.run("rpm -i %s" % self.build_file)

               
@job
def pkg_rpm():
    """create a redhat package

    This job creates combined source & binary package.
    """
    packager = RPM_Packager()
    packager()


@job
def pypi():
    """upload the source distribution to PyPI

    This job uploads `zip` and `tar.gz` source distributions
    to PyPI.  The distributions must be already built with
    `job pkg-src`.
    """
    if not (ls("./build/pkg/src/HTSQL-*.tar.gz") and
            ls("./build/pkg/src/HTSQL-*.zip")):
        raise fatal("cannot find source packages; run `job pkg-src` first")
    if os.path.exists("./build/tmp"):
        rmtree("./build/tmp")
    mktree("./build/tmp")
    archives = []
    for tgzname in ls("./build/pkg/src/*.tar.gz"):
        dirname = tgzname[:-7]
        zipname = dirname+".zip"
        dirname = os.path.basename(dirname)
        project, version = dirname.rsplit('-', 1)
        dirname = "./build/tmp/"+dirname
        run("tar -xzf %s -C ./build/tmp" % tgzname)
        mktree(dirname+"/dist")
        cp(tgzname, dirname+"/dist")
        cp(zipname, dirname+"/dist")
        setup_py("sdist --formats=zip,gztar --dry-run"
                 " register upload --sign --identity="+KEYSIG,
                 cd=dirname)
        archives.append((project, version))
    rmtree("./build/tmp")
    log()
    log("Source distribution archives are uploaded to:")
    for project, version in archives:
        log("  `http://pypi.python.org/pypi/%s/%s/`" % (project, version))
    log()


