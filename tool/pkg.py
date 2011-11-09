#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from job import job, log, debug, fatal, warn, run, rmtree, mktree, pipe
from vm import LinuxBenchVM, WindowsBenchVM, DATA_ROOT, CTL_DIR
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

def register_signing_key(vm):
    """ registers the HTSQL signing key in the given VM """
    pubkey = pipe("gpg --armour --export %s" % KEYSIG)
    seckey = pipe("gpg --armour --export-secret-key %s" % KEYSIG)
    vm.write("/root/sign.key", pubkey + seckey)
    vm.run("gpg --import /root/sign.key")

@job
def pkg_deb():
    """create a debian package

    This job creates combined debian combined source & binary package.
    You must also append a release note to tool/data/pkg/debian/changelog
    that matches the current release version.  Debian's pre-release naming
    convention is different than Python's since it requires a tilde 
    before the b1, rc1, etc.
    """
    if deb_vm.missing():
        raise fatal("VM is not built: %s" % deb_vm.name)
    source_paths = glob.glob("./build/pkg/src/HTSQL-*.tar.gz")
    if not source_paths:
        raise fatal("cannot find a source package; run `job pkg-src` first")
    if len(source_paths) > 1:
        raise fatal("too many source packages in `./build/pkg/src`")
    [source_path] = source_paths
    archive = os.path.basename(source_path)
    upstream_version = re.match(r'^HTSQL-(?P<version>.+).tar.gz$',
                                archive).group('version')
    (version, suffix) = re.match(r'^(\d+\.\d+.\d+)(.*)$',
                                 upstream_version).groups()
    if suffix:
        version = version + "~" + suffix
    changelog = open(DATA_ROOT+"/pkg/debian/changelog").read()
    if ('htsql (%s-1)' % version) not in changelog:
        raise fatal("update debian/changelog for %s release" % version)
    if deb_vm.running():
        deb_vm.stop()
    if os.path.exists("build/pkg/deb"):
        rmtree("build/pkg/deb")
    deb_vm.start()
    try:
        register_signing_key(deb_vm)
        deb_vm.put(source_path, '/root')
        deb_vm.run("mv %s htsql_%s.orig.tar.gz" % (archive, version))
        deb_vm.run("tar xvfz htsql_%s.orig.tar.gz" % version)
        deb_vm.put(DATA_ROOT+"/pkg/debian", "HTSQL-%s" % upstream_version)
        deb_vm.run("cd HTSQL-%s && dpkg-buildpackage -k%s" % \
                     (upstream_version, KEYSIG))
        mktree("build/pkg/deb")
        deb_vm.get("htsql_%s-1_all.deb" % version, "build/pkg/deb")
        deb_vm.run("dpkg -i htsql_%s-1_all.deb" % version)
        deb_vm.run("htsql-ctl get sqlite:test.db /%27Hello%20World%27")
    finally:
        deb_vm.stop()
    log()
    log("Debian package is built successfully:")
    log("  `./build/pkg/deb/htsql_%s-1_all.deb`" % version)
    log()


