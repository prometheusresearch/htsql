#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from job import job, log, debug, fatal, warn, run, rmtree, mktree
from vm import LinuxBenchVM, WindowsBenchVM, DATA_ROOT, CTL_DIR
import os, glob


src_vm = LinuxBenchVM('src', 'debian', 22)
rpm_vm = LinuxBenchVM('rpm', 'centos', 22)
deb_vm = LinuxBenchVM('deb', 'debian', 22)


VERSION = '2.2.0b2'
ARCHIVE = 'HTSQL-%s.tar.gz' % VERSION
PYPISRC = 'http://pypi.python.org/packages/source/H/HTSQL/%s' % ARCHIVE


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


@job
def pkg_deb():
    """create debian package

    This job creates the debian source and binary packages.
    """
    if deb_vm.missing():
       raise fatal("VM deb is not built")
    if deb_vm.running():
       deb_vm.stop()
    if os.path.exists("build/pkg/deb"):
       rmtree("build/pkg/deb")
    deb_vm.start()
    src_path = deb_vm.download([PYPISRC])
    deb_vm.put(src_path, '/root')
    deb_vm.run("mv /root/%s /root/htsql_%s.orig.tar.gz" % (ARCHIVE, VERSION))
    deb_vm.run("tar xvfz /root/htsql_%s.orig.tar.gz -C /root" % VERSION)
    deb_vm.put(DATA_ROOT+"/pkg/debian", "/root/HTSQL-%s" % VERSION)
    deb_vm.run("cd /root/HTSQL-%s && dpkg-buildpackage" % VERSION)
    mktree("build/pkg/deb")
    deb_vm.get("/root/htsql_%s-1_all.deb" % VERSION, "build/pkg/deb")
    deb_vm.run("dpkg -i /root/htsql_%s-1_all.deb" % VERSION)
    deb_vm.run("htsql-ctl get sqlite:test.db /%27Hello%20World%27")
    deb_vm.stop()
    log()
    log("`build/pkg/deb/htsql_%s-1_all.deb` is created successfully" % VERSION)

