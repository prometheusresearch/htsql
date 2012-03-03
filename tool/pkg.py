#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from job import job, log, debug, fatal, warn, run, cp, rmtree, mktree, pipe
from vm import LinuxBenchVM, WindowsBenchVM, DATA_ROOT, CTL_DIR
from dist import pipe_python, setup_py
import os, glob, re, StringIO, pprint
import setuptools
import yaml


KEYSIG = '8E70D862' # identifier for HTSQL package signing key
src_vm = LinuxBenchVM('src', 'debian', 22)
rpm_vm = LinuxBenchVM('rpm', 'centos', 22)
deb_vm = LinuxBenchVM('deb', 'debian', 22)


def make_setup(name, version, description=None, long_description=None,
               author=None, author_email=None, license=None, keywords=None,
               platform=None, url=None, classifiers=None, package_dir=None,
               packages=None, include_package_data=None, zip_safe=None,
               install_requires=None, console_scripts=None, addons=None,
               readme_file=None, license_file=None):
    data = []
    if readme_file:
        readme_data = "\n".join(open(DATA_ROOT+"/pkg/source/"+filename)
                                       .read().strip()+"\n\n"
                                for filename in readme_file)
        data.append(("README", readme_data))
    if license_file:
        license_data = "\n".join(open(DATA_ROOT+"/pkg/source/"+filename)
                                        .read().strip()+"\n\n"
                                 for filename in license_file)
        data.append(("LICENSE", license_data))
    stream = StringIO.StringIO()
    stream.write("#\n")
    stream.write("# This is a setup script for %s-%s.\n" % (name, version))
    stream.write("# Type `python setup.py install` to install %s.\n" % name)
    stream.write("# This file was generated automatically.\n")
    stream.write("#\n")
    stream.write("\n")
    stream.write("from setuptools import setup\n")
    stream.write("\n")
    stream.write("setup(\n")
    stream.write("    name=%r,\n" % name)
    stream.write("    version=%r,\n" % version)
    if description is not None:
        stream.write("    description=%r,\n" % description)
    if long_description is not None:
        stream.write("    long_description=%r,\n" % long_description)
    elif readme_file:
        stream.write("    long_description=open('README').read(),\n")
    if author is not None:
        stream.write("    author=%r,\n" % author)
    if author_email is not None:
        stream.write("    author_email=%r,\n" % author_email)
    if license is not None:
        stream.write("    license=%r,\n" % license)
    if keywords is not None:
        stream.write("    keywords=%r,\n" % keywords)
    if platform is not None:
        stream.write("    platform=%r,\n" % platform)
    if url is not None:
        stream.write("    url=%r,\n" % url)
    if classifiers is not None:
        stream.write("    classifiers=%s,\n" % pprint.pformat(classifiers))
    if package_dir is not None:
        stream.write("    package_dir=%r,\n" % package_dir)
    if packages is not None:
        all_packages = [package for package in setuptools.find_packages('src')
                        if package.split('.', 1)[0] in packages]
        stream.write("    packages=%r,\n" % all_packages)
    if include_package_data is not None:
        stream.write("    include_package_data=%r,\n" % include_package_data)
    if zip_safe is not None:
        stream.write("    zip_safe=%r,\n" % zip_safe)
    if install_requires is not None:
        stream.write("    install_requires=%r,\n" % install_requires)
    entry_points = {}
    if console_scripts is not None:
        entry_points['console_scripts'] = console_scripts
    if addons is not None:
        addons = [addon for addon in addons
                  if addon.split('=', 1)[1].strip().split('.', 1)[0]
                                                        in packages]
        if addons:
            entry_points['htsql.addons'] = addons
    if entry_points:
        stream.write("    entry_points=%s,\n" % pprint.pformat(entry_points))
    stream.write(")\n")
    stream.write("\n")
    data.append(("setup.py", stream.getvalue()))
    return data


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
    version = yaml.load(pipe_python("-c 'import setup, yaml;"
                                    " print yaml.dump(setup.get_version())'"))
    addons = yaml.load(pipe_python("-c 'import setup, yaml;"
                                   " print yaml.dump(setup.get_addons())'"))
    cfg_all = list(yaml.load_all(open(DATA_ROOT+"/pkg/source/setup.yaml").read()))
    with_doc = True
    src_vm.start()
    try:
        cfg_common = cfg_all[0]
        for cfg in cfg_all[1:]:
            for key in cfg_common:
                if key not in cfg:
                    cfg[key] = cfg_common[key]
            for key, value in cfg.items():
                if '-' in key:
                    del cfg[key]
                    key = key.replace('-', '_')
                    cfg[key] = value
            cfg['version'] = version
            cfg['addons'] = addons
            extra = make_setup(**cfg)
            src_vm.forward(22)
            run("hg clone --ssh='ssh -F %s' . ssh://linux-vm/htsql"
                % (CTL_DIR+"/ssh_config"))
            src_vm.unforward(22)
            src_vm.run("cd htsql && hg update")
            if with_doc:
                src_vm.run("cd htsql &&"
                           " PYTHONPATH=src sphinx-build -d doc doc doc/html")
                with_doc = False
            for filename, data in extra:
                src_vm.write("htsql/"+filename, data)
            src_vm.run("cd htsql && python setup.py sdist --formats=zip,gztar")
            if not os.path.exists("./build/pkg/src"):
                mktree("./build/pkg/src")
            src_vm.get("./htsql/dist/*", "./build/pkg/src")
            src_vm.run("rm -rf htsql")
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
        raise fatal("cannot find source packages; run `job pkg-src` first")
    if os.path.exists("./build/pkg/stage"):
        rmtree("./build/pkg/stage")
    mktree("./build/pkg/stage")
    archives = []
    for tgzname in sorted(glob.glob("./build/pkg/src/*.tar.gz")):
        dirname = tgzname[:-7]
        zipname = dirname+".zip"
        dirname = os.path.basename(dirname)
        project, version = dirname.rsplit('-', 1)
        dirname = "./build/pkg/stage/"+dirname
        run("tar -xzf %s -C ./build/pkg/stage" % tgzname)
        mktree(dirname+"/dist")
        cp(tgzname, dirname+"/dist")
        cp(zipname, dirname+"/dist")
        setup_py("sdist --formats=zip,gztar --dry-run"
                 " register upload --sign --identity="+KEYSIG,
                 cd=dirname)
        archives.append((project, version))
    rmtree("./build/pkg/stage")
    log()
    log("Source distribution archives are uploaded to:")
    for project, version in archives:
        log("  `http://pypi.python.org/pypi/%s/%s/`" % (project, version))
    log()


