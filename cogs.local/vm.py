#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from cogs import task, setting, env
from cogs.fs import sh, pipe, exe, cp, mv, rm, mktree, rmtree
from cogs.log import log, debug, warn, fail, prompt
import os, os.path
import glob
import urllib.request, urllib.error, urllib.parse
import socket
import datetime
import time
import re
import tempfile


VM_ROOT = "./vm-build"
IMG_DIR = VM_ROOT+"/img"
CTL_DIR = VM_ROOT+"/ctl"
TMP_DIR = VM_ROOT+"/tmp"

DATA_ROOT = "./cogs.local/data"

DISK_SIZE = "8G"
MEM_SIZE = "512"

DEBIAN_ISO_URLS = [
    "http://cdimage.debian.org/cdimage/archive/6.0.6/i386/iso-cd/debian-6.0.6-i386-netinst.iso",
    "http://cdimage.debian.org/cdimage/release/6.0.6/i386/iso-cd/debian-6.0.6-i386-netinst.iso",
]

UBUNTU_ISO_URLS = [
    "http://releases.ubuntu.com/16.04.1/ubuntu-16.04.1-server-i386.iso",
]

CENTOS_ISO_URLS = [
    "http://mirrors.cmich.edu/centos/6.3/isos/i386/CentOS-6.3-i386-minimal.iso",
    "http://vault.centos.org/6.3/isos/i386/CentOS-6.3-i386-minimal.iso",
]

WINDOWS_ISO_FILES = [
    "en_win_srv_2003_r2_standard_with_sp2_cd1_x13-04790.iso",
    "en_windows_xp_professional_with_service_pack_3_x86_cd_x14-80428.iso",

]

WGET_EXE_URLS = [
    "http://downloads.sourceforge.net/gnuwin32/wget-1.11.4-1-setup.exe",
]

env.add(vms=[])


class VM(object):
    # A virtual machine.

    @classmethod
    def find(cls, name):
        # Get an instance by name.
        for vm in env.vms:
            if vm.name == name:
                return vm
        raise fail("unknown VM: {}", name)

    @classmethod
    def list(cls):
        # List all VM instances.
        return env.vms[:]

    def __init__(self, name, system, state=None):
        # Create an instance with the given name.
        assert not any(vm.name == name for vm in env.vms)
        self.name = name
        self.system = system
        self.state = state
        self.img_path = IMG_DIR+"/%s.qcow2" % name
        self.ctl_path = CTL_DIR+"/%s.ctl" % name
        env.vms.append(self)

    def missing(self):
        # Check if the VM is built.
        return (not os.path.isfile(self.img_path))

    def running(self):
        # Check if the VM is currently running.
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.connect(self.ctl_path)
            sock.close()
        except socket.error:
            return False
        return True

    def ports(self):
        # List ports forwarded by the VM.
        if not self.running():
            return []
        ports = []
        for filename in glob.glob(CTL_DIR+"/port.*"):
            name = open(filename).read().strip()
            if name == self.name:
                port = int(filename.rsplit(".", 1)[-1])
                ports.append(port)
        ports.sort()
        return ports

    def build(self):
        # Generate a VM image.
        if not self.missing():
            raise fail("VM is already built")
        for path in [IMG_DIR, CTL_DIR, TMP_DIR]:
            if not os.path.exists(path):
                mktree(path)
        identity_path = CTL_DIR+"/identity"
        if not os.path.exists(identity_path):
            sh("ssh-keygen -q -N \"\" -f %s" % identity_path)
        config_path = CTL_DIR+"/ssh_config"
        if not os.path.exists(config_path):
            config_template_path = DATA_ROOT+"/vm/ssh_config"
            config_template = open(config_template_path).read()
            config = config_template.replace("$VM_ROOT", VM_ROOT)
            assert config != config_template
            debug("translating: {} => {}", config_template_path, config_path)
            open(config_path, 'w').write(config)

    def delete(self):
        # Delete the VM image.
        if self.missing():
            raise fail("VM is not built: {}", self.name)
        if self.running():
            raise fail("VM is running: {}", self.name)
        log("deleting VM: {}", self.name)
        rm(self.img_path)
        if os.path.exists(self.ctl_path):
            rm(self.ctl_path)

    def start(self):
        # Start a VM.
        if self.missing():
            raise fail("VM is not built: {}", self.name)
        if self.running():
            raise fail("VM is already running: {}", self.name)
        log("starting VM: {}", self.name)
        for filename in glob.glob(CTL_DIR+"/port.*"):
            name = open(filename).read().strip()
            if name == self.name:
                rm(filename)
        if self.state:
            self.kvm("-daemonize -loadvm %s" % self.state)
        else:
            self.kvm("-daemonize -snapshot")

    def stop(self):
        # Stop a VM.
        if self.missing():
            raise fail("VM is not built: {}", self.name)
        if not self.running():
            raise fail("VM is not running: {}", self.name)
        log("stopping VM: {}", self.name)
        for port in self.ports():
            self.unforward(port)
        self.ctl("quit")
        self.wait()

    def download(self, urls):
        # Download a file from a list of URLs and save it to `./vm/tmp`.
        for url in urls:
            path = TMP_DIR+"/"+os.path.basename(url)
            if os.path.exists(path):
                return path
            data = None
            debug("downloading: {} => {}", url, path)
            try:
                data = urllib.request.urlopen(url).read()
            except urllib.error.HTTPError:
                pass
            if data is not None:
                stream = open(path, 'w')
                stream.write(data)
                stream.close()
                return path
        raise fail("failed to download: {}", ", ".join(urls))

    def unpack_iso(self, iso_path, target_path):
        # Unpack an ISO image.
        assert os.path.isfile(iso_path)
        if not os.path.exists(target_path):
            mktree(target_path)
        debug("unpacking: {} => {}", iso_path, target_path)
        listing = pipe("isoinfo -i %s -R -f" % iso_path)
        for entry in listing.splitlines():
            filename = target_path+entry
            dirname = os.path.dirname(filename)
            if not os.path.exists(dirname):
                mktree(dirname)
            with env(debug=False):
                content = pipe("isoinfo -i %s -R -x '%s'" % (iso_path, entry))
            if not content:
                continue
            #debug("extracting: {} => {}", entry, filename)
            stream = open(filename, 'w')
            stream.write(content)
            stream.close()

    def unpack_iso_boot(self, iso_path, boot_path):
        # Unpack El Torito boot image from and ISO.
        assert os.path.isfile(iso_path)
        debug("unpacking boot image: {} => {}", iso_path, boot_path)
        sh("geteltorito -o %s %s" % (boot_path, iso_path))

    def wait(self):
        # Wait till the VM stops.
        while self.running():
            time.sleep(1.0)

    def ctl(self, command):
        # Send a command to the VM.
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.ctl_path)
        time.sleep(0.1)
        sock.recv(4096)
        for line in command.splitlines():
            debug("writing: {} << {}", self.ctl_path, line)
        sock.send(command+"\n")
        time.sleep(0.1)
        data = sock.recv(4096)
        for line in data.splitlines():
            debug("reading: {} >> {}", self.ctl_path, line)
        sock.close()

    def forward(self, port):
        # Forward a port from a local host to the VM.
        local_port = port+10000
        debug("forwarding: localhost:{} -> {}:{}", local_port, self.name, port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(('127.0.0.1', local_port))
            sock.close()
            is_taken = True
        except socket.error:
            is_taken = False
        port_path = CTL_DIR+"/port.%s" % port
        if is_taken:
            if os.path.exists(port_path):
                name = open(port_path).read().strip()
                if name != self.name:
                    vm = VM.find(name)
                    if not vm.running():
                        raise fail("unable to forward port: {} -> {}:{}",
                                   local_port, self.name, port)
                    raise fail("unable to forward port: {} -> {}:{}"
                               " (already forwarded by VM {})",
                               local_port, self.name, port, name)
            else:
                raise fail("unable to forward port: {} -> {}:{}",
                           local_port, self.name, port)
        else:
            self.ctl("hostfwd_add tcp:127.0.0.1:%s-:%s"
                     % (local_port, port))
            open(port_path, 'w').write("%s\n" % self.name)

    def unforward(self, port):
        # Remove a forwarding rule.
        local_port = port+10000
        debug("unforwarding: localhost:{} -> {}:{}",
              local_port, self.name, port)
        self.ctl("hostfwd_remove tcp:127.0.0.1:%s-:%s" % (local_port, port))
        port_path = CTL_DIR+"/port.%s" % port
        if os.path.exists(port_path):
            rm(port_path)

    def kvm_img(self):
        # Run `qemu-img create -f qcow2 <img_path> <DISK_SIZE>`.
        sh("qemu-img create -f qcow2 %s %s" % (self.img_path, DISK_SIZE))

    def compress(self, backing_name=None):
        # Run `qemu-img convert -c ...`.
        opts = "convert -c -f qcow2 -O qcow2"
        if backing_name:
            opts += " -o backing_file=%s.qcow2" % backing_name
        sh("qemu-img %s %s.qcow2 %s-compressed.qcow2"
           % (opts, self.name, self.name), cd=IMG_DIR)
        mv(IMG_DIR+"/%s-compressed.qcow2" % self.name,
           IMG_DIR+"/%s.qcow2" % self.name)

    def kvm(self, opts=None):
        # Run `kvm`.
        net_model = "virtio"
        if self.system == 'windows':
            net_model = "rtl8139"
        sh("kvm -name %s -monitor unix:%s,server,nowait"
           " -drive file=%s,cache=writeback"
           " -net nic,model=%s -net user -vga cirrus"
           " -rtc clock=vm -m %s"
           % (self.name, self.ctl_path, self.img_path, 
              net_model, MEM_SIZE)
           + ((" "+opts) if opts else "")
           + ("" if env.debug else " -vnc none"))

    def run(self, command):
        # Run a shell command in the VM.
        self.forward(22)
        host = "linux-vm"
        if self.system == 'windows':
            host = "windows-vm"
        sh("ssh -F %s %s \"%s\"" % (CTL_DIR+"/ssh_config", host, command))
        self.unforward(22)

    def put(self, src_filename, dst_filename):
        # Copy a file to the VM.
        self.forward(22)
        host = "linux-vm"
        if self.system == 'windows':
            host = "windows-vm"
        sh("scp -rF %s \"%s\" %s:\"%s\""
           % (CTL_DIR+"/ssh_config", src_filename, host, dst_filename))
        self.unforward(22)

    def write(self, dst_filename, content):
        # Create a file with given content.
        try:
            tf = tempfile.NamedTemporaryFile()
            tf.write(content)
            tf.flush()
            self.put(tf.name, dst_filename)
        finally:
            tf.close()

    def get(self, src_filename, dst_filename):
        # Read a file from VM.
        self.forward(22)
        host = "linux-vm"
        if self.system == 'windows':
            host = "windows-vm"
        sh("scp -rF %s %s:\"%s\" \"%s\""
           % (CTL_DIR+"/ssh_config", host, src_filename, dst_filename))
        self.unforward(22)


class DebianTemplateVM(VM):
    # Debian 6.0 "squeeze" (32-bit) VM.

    def __init__(self, name, iso_env='debian_iso', iso_urls=DEBIAN_ISO_URLS):
        super(DebianTemplateVM, self).__init__(name, 'linux')
        self.iso_env = iso_env
        self.iso_urls = iso_urls

    def build(self):
        super(DebianTemplateVM, self).build()
        log("building VM: `{}`...", self.name)
        start_time = datetime.datetime.now()
        src_iso_path = getattr(env, self.iso_env)
        if not (src_iso_path and os.path.isfile(src_iso_path)):
            src_iso_path = self.download(self.iso_urls)
        unpack_path = TMP_DIR+"/"+self.name
        if os.path.exists(unpack_path):
            rmtree(unpack_path)
        self.unpack_iso(src_iso_path, unpack_path)
        cp(DATA_ROOT+"/vm/%s-isolinux.cfg" % self.name,
           unpack_path+"/isolinux/isolinux.cfg")
        cp(DATA_ROOT+"/vm/%s-preseed.cfg" % self.name,
           unpack_path+"/preseed.cfg")
        cp(DATA_ROOT+"/vm/%s-install.sh" % self.name,
           unpack_path+"/install.sh")
        cp(CTL_DIR+"/identity.pub", unpack_path+"/identity.pub")
        sh("md5sum"
           " `find ! -name \"md5sum.txt\""
           " ! -path \"./isolinux/*\" -follow -type f` > md5sum.txt",
           cd=unpack_path)
        iso_path = TMP_DIR+"/%s.iso" % self.name
        if os.path.exists(iso_path):
            rm(iso_path)
        sh("mkisofs -o %s"
           " -q -r -J -no-emul-boot -boot-load-size 4 -boot-info-table"
           " -b isolinux/isolinux.bin -c isolinux/boot.cat %s"
           % (iso_path, unpack_path))
        rmtree(unpack_path)
        try:
            self.kvm_img()
            self.kvm("-cdrom %s -boot d" % iso_path)
            rm(iso_path)
            self.compress()
        except:
            if os.path.exists(self.img_path):
                rm(self.img_path)
            raise
        stop_time = datetime.datetime.now()
        log("VM is built successfully: `{}` ({})",
            self.name, stop_time-start_time)


class CentOSTemplateVM(VM):
    # CentOS 6.2 (32-bit) VM.

    def __init__(self, name):
        super(CentOSTemplateVM, self).__init__(name, 'linux')

    def build(self):
        super(CentOSTemplateVM, self).build()
        log("building VM: `{}`...", self.name)
        start_time = datetime.datetime.now()
        src_iso_path = env.centos_iso
        if not (src_iso_path and os.path.isfile(src_iso_path)):
            src_iso_path = self.download(CENTOS_ISO_URLS)
        unpack_path = TMP_DIR+"/"+self.name
        if os.path.exists(unpack_path):
            rmtree(unpack_path)
        self.unpack_iso(src_iso_path, unpack_path)
        cp(DATA_ROOT+"/vm/%s-isolinux.cfg" % self.name,
           unpack_path+"/isolinux/isolinux.cfg")
        cp(DATA_ROOT+"/vm/%s-ks.cfg" % self.name, unpack_path+"/ks.cfg")
        cp(DATA_ROOT+"/vm/%s-install.sh" % self.name, unpack_path+"/install.sh")
        cp(CTL_DIR+"/identity.pub", unpack_path+"/identity.pub")
        iso_path = TMP_DIR+"/%s.iso" % self.name
        if os.path.exists(iso_path):
            rm(iso_path)
        sh("mkisofs -o %s"
           " -q -r -J -T -no-emul-boot -boot-load-size 4 -boot-info-table"
           " -b isolinux/isolinux.bin -c isolinux/boot.cat %s"
           % (iso_path, unpack_path))
        rmtree(unpack_path)
        try:
            self.kvm_img()
            self.kvm("-cdrom %s -boot d" % iso_path)
            rm(iso_path)
            self.compress()
        except:
            if os.path.exists(self.img_path):
                rm(self.img_path)
            raise
        stop_time = datetime.datetime.now()
        log("VM is built successfully: `{}` ({})",
            (self.name, stop_time-start_time))


class WindowsTemplateVM(VM):
    # Windows XP/2003 (32-bit) VM.

    def __init__(self, name):
        super(WindowsTemplateVM, self).__init__(name, 'windows')

    def build(self):
        super(WindowsTemplateVM, self).build()
        log("building VM: `{}`...", self.name)
        start_time = datetime.datetime.now()
        src_iso_path = env.windows_iso
        if not (src_iso_path and os.path.isfile(src_iso_path)):
            src_iso_path = None
            output = pipe("locate %s || true"
                          % " ".join(WINDOWS_ISO_FILES))
            for line in output.splitlines():
                if os.path.exists(line):
                    src_iso_path = line
                    break
        if src_iso_path is None:
            log("unable to find an ISO image for Windows XP or Windows 2003")
            src_iso_path = prompt("enter path to an ISO image:")
            if not (src_iso_path and os.path.isfile(src_iso_path)):
                raise fail("invalid path: %s" % src_iso_path)
        key_regexp = re.compile(r'^\w{5}-\w{5}-\w{5}-\w{5}-\w{5}$')
        key = env.windows_key
        if not (key and key_regexp.match(key)):
            key = None
            key_path = os.path.splitext(src_iso_path)[0]+".key"
            if os.path.isfile(key_path):
                key = open(key_path).readline().strip()
                if not key_regexp.match(key):
                    key = None
        if key is None:
            log("unable to find a Windows product key")
            key = prompt("enter product key:")
            if not key_regexp.match(key):
                raise fail("invalid product key: {}", key)
        wget_path = self.download(WGET_EXE_URLS)
        unpack_path = TMP_DIR+"/"+self.name
        boot_path = unpack_path+"/eltorito.img"
        if os.path.exists(unpack_path):
            rmtree(unpack_path)
        self.unpack_iso(src_iso_path, unpack_path)
        self.unpack_iso_boot(src_iso_path, boot_path)
        sif_template_path = DATA_ROOT+"/vm/%s-winnt.sif" % self.name
        sif_path = unpack_path+"/I386/WINNT.SIF"
        debug("translating: {} => {}", sif_template_path, sif_path)
        sif_template = open(sif_template_path).read()
        sif = sif_template.replace("#####-#####-#####-#####-#####", key)
        assert sif != sif_template
        open(sif_path, 'w').write(sif)
        install_path = unpack_path+"/$OEM$/$1/INSTALL"
        mktree(install_path)
        cp(wget_path, install_path)
        cp(CTL_DIR+"/identity.pub", install_path)
        cp(DATA_ROOT+"/vm/%s-install.cmd" % self.name, install_path+"/INSTALL.CMD")
        iso_path = TMP_DIR+"/%s.iso" % self.name
        if os.path.exists(iso_path):
            rm(iso_path)
        sh("mkisofs -o %s -q -iso-level 2 -J -l -D -N"
           " -joliet-long -relaxed-filenames -no-emul-boot"
           " -boot-load-size 4 -b eltorito.img %s"
           % (iso_path, unpack_path))
        rmtree(unpack_path)
        try:
            self.kvm_img()
            self.kvm("-cdrom %s -boot d" % iso_path)
            rm(iso_path)
            self.compress()
        except:
            if os.path.exists(self.img_path):
                rm(self.img_path)
            raise
        stop_time = datetime.datetime.now()
        log("VM is built successfully: `{}` ({})",
            self.name, stop_time-start_time)


class LinuxBenchVM(VM):
    # A linux-based working VM.

    def __init__(self, name, parent, port):
        super(LinuxBenchVM, self).__init__(name, 'linux', 'begin')
        self.parent = parent
        self.port = port

    def build(self):
        super(LinuxBenchVM, self).build()
        parent_vm = VM.find(self.parent)
        if parent_vm.missing():
            parent_vm.build()
        if parent_vm.running():
            raise fail("unable to copy VM while it is running: {}",
                       parent_vm.name)
        log("building VM: `{}`...", self.name)
        start_time = datetime.datetime.now()
        try:
            sh("qemu-img create -b %s.qcow2 -f qcow2 %s.qcow2"
               % (parent_vm.name, self.name), cd=IMG_DIR)
            self.kvm("-daemonize")
            time.sleep(60.0)
            self.put(DATA_ROOT+"/vm/%s-update.sh" % self.name, "/root/update.sh")
            self.run("/root/update.sh")
            self.run("rm /root/update.sh")
            self.run("shutdown")
            self.wait()
            #self.compress(parent_vm.name)
            self.kvm("-daemonize")
            time.sleep(60.0)
            self.ctl("savevm %s" % self.state)
            self.ctl("quit")
            self.wait()
        except:
            if self.running():
                self.ctl("quit")
                self.wait()
            if os.path.exists(self.img_path):
                rm(self.img_path)
            raise
        stop_time = datetime.datetime.now()
        log("VM is built successfully: `{}` ({})",
            self.name, stop_time-start_time)

    def start(self):
        super(LinuxBenchVM, self).start()
        try:
            self.forward(self.port)
        except:
            self.ctl("quit")
            self.wait()
            raise

    def unforward(self, port):
        if port == self.port:
            return
        super(LinuxBenchVM, self).unforward(port)


class WindowsBenchVM(VM):
    # A windows-based working VM.

    def __init__(self, name, parent, port):
        super(WindowsBenchVM, self).__init__(name, 'windows', 'begin')
        self.parent = parent
        self.port = port

    def build(self):
        super(WindowsBenchVM, self).build()
        parent_vm = VM.find(self.parent)
        if parent_vm.missing():
            parent_vm.build()
        if parent_vm.running():
            raise fail("unable to copy VM while it is running: {}",
                       parent_vm.name)
        log("building VM: `{}`...", self.name)
        start_time = datetime.datetime.now()
        try:
            cp(parent_vm.img_path, self.img_path)
            self.kvm("-daemonize")
            time.sleep(120.0)
            self.put(DATA_ROOT+"/vm/%s-update.cmd" % self.name,
                     "/cygdrive/c/INSTALL/UPDATE.CMD")
            self.run("reg add 'HKLM\Software\Microsoft\Windows\CurrentVersion\RunOnce'"
                     " /v %s /t REG_SZ /d 'C:\INSTALL\\UPDATE.CMD' /f" % self.name)
            self.run("shutdown /r /t 0 /f")
            self.wait()
            #self.compress(parent_vm.name)
            self.kvm("-daemonize")
            time.sleep(120.0)
            self.ctl("savevm %s" % self.state)
            self.ctl("quit")
            self.wait()
        except:
            if self.running():
                self.ctl("quit")
                self.wait()
            if os.path.exists(self.img_path):
                rm(self.img_path)
            raise
        stop_time = datetime.datetime.now()
        log("VM is built successfully: `{}` ({})",
            self.name, stop_time-start_time)

    def start(self):
        super(WindowsBenchVM, self).start()
        try:
            self.forward(self.port)
        except:
            self.ctl("quit")
            self.wait()
            raise

    def unforward(self, port):
        if port == self.port:
            return
        super(WindowsBenchVM, self).unforward(port)


debian_vm = DebianTemplateVM('debian')
centos_vm = CentOSTemplateVM('centos')
windows_vm = WindowsTemplateVM('windows')
ubuntu_vm = DebianTemplateVM('ubuntu',
                             iso_env="ubuntu_iso",
                             iso_urls=UBUNTU_ISO_URLS)


@setting
def DEBIAN_ISO(path=None):
    """path to Debian 6.0 CDROM image"""
    env.add(debian_iso=path or None)


@setting
def UBUNTU_ISO(path=None):
    """path to Ubuntu 16.04 CDROM image"""
    env.add(ubuntu_iso=path or None)


@setting
def CENTOS_ISO(path=None):
    """path to CentOS 6 CDROM image"""
    env.add(centos_iso=path or None)


@setting
def WINDOWS_ISO(path=None):
    """path to MS Windows XP or 2003 CDROM image"""
    env.add(windows_iso=path or None)


@setting
def WINDOWS_KEY(key=None):
    """registration key for MS Windows CDROM image"""
    env.add(windows_key=key or None)


@task
def VM_LIST():
    """list all virtual machines

    This task lists all registered virtual machines and their states.
    A VM could be in one of three states:

    - `missing`: the VM image is not built;
    - `stopped`: the VM image exists; the VM is not active.
    - `running`: the VM image exists and the VM is active.

    For running VMs, the task lists all ports forwarded from the local
    host to the VM.
    """
    log("Available virtual machines:")
    for vm in VM.list():
        status = 'missing'
        if not vm.missing():
            status = 'stopped'
        if vm.running():
            status = 'running'
            ports = vm.ports()
            if ports:
                status += " (%s)" \
                        % (", ".join("%s -> %s" % (port+10000, port)
                                     for port in ports))
        log("  {:<24} : {}", vm.name, status)
    log()


@task
def VM_BUILD(*names):
    """build a virtual machine

    This task builds a virtual machine image from scratch.  This
    usually takes some time and may require the original ISO
    image and a product key of the operating system.

    Run this task without arguments to build images for all
    registered VMs.
    """
    if names:
        vms = [VM.find(name) for name in names]
    else:
        vms = [vm for vm in VM.list() if vm.missing()]
    for vm in vms:
        if not vm.missing():
            warn("VM is already built: {}", vm.name)
            continue
        vm.build()


@task
def VM_DELETE(*names):
    """delete a virtual machine

    This task deletes an existing virtual machine image.
    """
    if not names:
        raise fail("VM is not specified")
    vms = [VM.find(name) for name in names]
    for vm in vms:
        if vm.missing():
            warn("VM is not built: {}", vm.name)
            continue
        if vm.running():
            warn("VM is running: {}", vm.name)
            continue
        vm.delete()


@task
def VM_START(*names):
    """start a virtual machine

    This task starts a virtual machine.
    """
    if not names:
        raise fail("VM is not specified")
    vms = [VM.find(name) for name in names]
    for vm in vms:
        if vm.running():
            warn("VM is already running: {}", vm.name)
            continue
        vm.start()


@task
def VM_STOP(*names):
    """stop a virtual machine

    This task stops a running virtual machine.  Run this task without
    any arguments to stop all running VMs.
    """
    if names:
        vms = [VM.find(name) for name in names]
    else:
        vms = [vm for vm in VM.list() if vm.running()]
    for vm in vms:
        if not vm.running():
            warn("VM is not running: {}", vm.name)
            continue
        vm.stop()


@task
def VM_SSH(name):
    """open a shell to a virtual machine

    Open an SSH session to a running virtual machine.
    """
    vm = VM.find(name)
    if not vm.running():
        raise fail("VM is not running: {}", vm.name)
    vm.forward(22)
    host = "linux-vm"
    if vm.system == 'windows':
        host = "windows-vm"
    with env(debug=True):
        exe("ssh -F %s %s" % (CTL_DIR+"/ssh_config", host))


@task
def VM_CTL(name, cmd):
    """send a command to a virtual machine

    This task sends a low-level command to the virtual machine monitor.
    For the list of commands, see:
      `http://en.wikibooks.org/wiki/QEMU/Monitor`
    """
    vm = VM.find(name)
    if not vm.running():
        raise fail("VM is not running: {}", vm.name)
    vm.ctl(cmd)


