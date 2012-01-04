#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from job import job, log, debug, fatal, warn, run, rmtree, mktree, pipe
from vm import LinuxBenchVM, WindowsBenchVM, DATA_ROOT, CTL_DIR
from dist import setup_py
import os, glob, re

KEYSIG = '8E70D862' # identifier for HTSQL package signing key
src_vm = LinuxBenchVM('src', 'debian', 22)
rpm_vm = LinuxBenchVM('rpm', 'centos', 22)
deb_vm = LinuxBenchVM('deb', 'debian', 22)


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
    src_vm.start()
    try:
        run("hg clone --ssh='ssh -F %s' . ssh://linux-vm/htsql"
            % (CTL_DIR+"/ssh_config"))
        src_vm.run("cd htsql && hg update")
        src_vm.run("cd htsql && python setup.py sdist --formats=zip,gztar")
        src_vm.run("tar -xzf htsql/dist/HTSQL-*.tar.gz")
        src_vm.run("cd HTSQL-* && python setup.py install")
        src_vm.run("htsql-ctl get sqlite:test.sqlite /%27Hello%20World%27")
        if not os.path.exists("./build/pkg"):
            mktree("./build/pkg")
        src_vm.get("./htsql/dist", "./build/pkg/src")
    finally:
        src_vm.stop()
    log()
    log("The generated source packages are placed in:")
    for filename in sorted(glob.glob("./build/pkg/src/*")):
        log("  `%s`" % filename)
    log()


class Packager(object):

    def __init__(self, vm):
        self.vm = vm
        if self.vm.missing():
             raise fatal("VM is not built: %s" % self.vm.name)
        source_paths = glob.glob("./build/pkg/src/HTSQL-*.tar.gz")
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
    if not (glob.glob("./build/pkg/src/HTSQL-*.tar.gz") and
            glob.glob("./build/pkg/src/HTSQL-*.zip")):
        raise fatal("cannot find a source package; run `job pkg-src` first")
    setup_py("sdist --formats=zip,gztar --dist-dir=build/pkg/src --dry-run"
             " register upload --sign --identity="+KEYSIG)
    log()
    log("Source distribution archives are uploaded to:")
    log("  `http://pypi.python.org/pypi/HTSQL/`")
    log()


