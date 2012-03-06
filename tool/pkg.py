#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from job import job, log, debug, fatal, warn, run, ls, cp, rmtree, mktree, pipe
from vm import LinuxBenchVM, WindowsBenchVM, DATA_ROOT, CTL_DIR
from dist import pipe_python, setup_py
import os, re, shutil
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
            origin = None
            if parts is None:
                parts = []
            elif isinstance(parts, str):
                origin = parts
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
            stream.close()
            if origin:
                shutil.copystat(os.path.join(src, origin), filename)

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


def get_version():
    return yaml.load(pipe_python("-c 'import setup, yaml;"
                                 " print yaml.dump(setup.get_version())'"))

def get_addons():
    return yaml.load(pipe_python("-c 'import setup, yaml;"
                                 " print yaml.dump(setup.get_addons())'"))


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
    version = get_version()
    all_addons = get_addons()
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


@job
def pkg_deb():
    """create Debian packages

    This job creates Debian packages from source packages.
    """
    if deb_vm.missing():
        raise fatal("VM is not built: %s" % deb_vm.name)
    if deb_vm.running():
        deb_vm.stop()
    if os.path.exists("./build/pkg/deb"):
        rmtree("./build/pkg/deb")
    if os.path.exists("./build/tmp"):
        rmtree("./build/tmp")
    version = get_version()
    debian_version = version
    if len(version.split(".")) > 3:
        debian_version = (".".join(version.split(".")[:3]) + "~" +
                          ".".join(version.split(".")[3:]))
    moves = load_moves(DATA_ROOT+"/pkg/debian/moves.yaml")
    deb_vm.start()
    pubkey = pipe("gpg --armour --export %s" % KEYSIG)
    seckey = pipe("gpg --armour --export-secret-key %s" % KEYSIG)
    deb_vm.write("/root/sign.key", pubkey + seckey)
    deb_vm.run("gpg --import /root/sign.key")
    try:
        for move in moves:
            package = "%s-%s" % (move.code.upper(), version)
            debian_package = "%s_%s" % (move.code, debian_version)
            archive = "./build/pkg/src/%s.tar.gz" % package
            if not os.path.exists(archive):
                raise fatal("cannot find a source package;"
                            " run `job pkg-src` first")
            changelog = open(DATA_ROOT+"/pkg/debian/changelog").read()
            if ('htsql (%s-1)' % debian_version) not in changelog:
                raise fatal("run `job pkg-deb-changelog`"
                            " to update the changelog file")
            changelog = changelog.replace('htsql (', '%s (' % move.code)
            mktree("./build/tmp")
            cp(archive, "./build/tmp/%s.orig.tar.gz" % debian_package)
            run("tar -xzf %s -C ./build/tmp" % archive)
            move(DATA_ROOT+"/pkg/debian", "./build/tmp/%s" % package)
            open("./build/tmp/%s/debian/changelog" % package, 'w') \
                    .write(changelog)
            deb_vm.put("./build/tmp", "./build")
            deb_vm.run("cd ./build/%s && dpkg-buildpackage -k%s"
                       % (package, KEYSIG))
            if not os.path.exists("./build/pkg/deb"):
                mktree("./build/pkg/deb")
            deb_vm.get("./build/*.deb", "./build/pkg/deb")
            deb_vm.run("rm -rf build")
            rmtree("./build/tmp")
    finally:
        deb_vm.stop()
    log()
    log("The generated Debian packages are placed in:")
    for filename in ls("./build/pkg/deb/*"):
        log("  `%s`" % filename)
    log()


@job
def pkg_deb_changelog(message=None):
    """update the Debian changelog

    This job updates the Debian changelog to the current HTSQL version.
    """
    if message is None:
        message = "new upstream release"
    version = get_version()
    debian_version = version
    if len(version.split(".")) > 3:
        debian_version = (".".join(version.split(".")[:3]) + "~" +
                          ".".join(version.split(".")[3:]))
    debian_version += "-1"
    changelog = open(DATA_ROOT+"/pkg/debian/changelog").read()
    if ('htsql (%s)' % debian_version) in changelog:
        raise fatal("changelog is already up-to-date")
    run("dch --check-dirname-level 0 -D unstable -v %s %s"
        % (debian_version, message),
        cd=DATA_ROOT+"/pkg/debian")
    log("The Debian changelog is updated to version:")
    log("  `%s`" % debian_version)
    log()


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


