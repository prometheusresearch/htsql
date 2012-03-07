#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from job import job, log, debug, fatal, warn, run
from vm import LinuxBenchVM, WindowsBenchVM, CTL_DIR
import sys, subprocess


py25_vm = LinuxBenchVM('py25', 'debian', 22)
py26_vm = LinuxBenchVM('py26', 'debian', 22)
py27_vm = LinuxBenchVM('py27', 'debian', 22)
pgsql84_vm = LinuxBenchVM('pgsql84', 'debian', 5432)
pgsql90_vm = LinuxBenchVM('pgsql90', 'debian', 5432)
pgsql91_vm = LinuxBenchVM('pgsql91', 'debian', 5432)
mysql51_vm = LinuxBenchVM('mysql51', 'debian', 3306)
oracle10g_vm = LinuxBenchVM('oracle10g', 'debian', 1521)
mssql2005_vm = WindowsBenchVM('mssql2005', 'windows', 1433)
mssql2008_vm = WindowsBenchVM('mssql2008', 'windows', 1433)


def trial(command, description):
    # Run a command on VM; complain if exited with non-zero error code.
    log("%s..." % description)
    command = "ssh -F %s linux-vm \"cd src/htsql && %s\"" \
              % (CTL_DIR+"/ssh_config", command)
    stream = subprocess.PIPE
    debug("trying: %s" % command)
    proc = subprocess.Popen(command, shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    out, err = proc.communicate()
    if proc.returncode == 0:
        log("%s: PASSED" % description)
        return 0
    out = out.strip()
    log("%s: [!!] FAILED!" % description)
    log("*"*72)
    sys.stdout.write(out+"\n")
    log("*"*72)
    return 1


@job
def check_all():
    """run regression tests for all supported backends

    This job runs HTSQL regression tests on all combinations of client
    and server platforms.
    """
    vms = [py25_vm, py26_vm, py27_vm, pgsql84_vm, pgsql90_vm, pgsql91_vm,
           mysql51_vm, oracle10g_vm, mssql2005_vm, mssql2008_vm]
    for vm in vms:
        if vm.missing():
            warn("VM is not built: %s" % vm.name)
    for vm in vms:
        if vm.running():
            vm.stop()
    errors = 0
    try:
        for client_vm in [py25_vm, py26_vm, py27_vm]:
            if client_vm.missing():
                continue
            client_vm.start()
            run("hg clone --ssh='ssh -F %s' . ssh://linux-vm/src/htsql"
                % (CTL_DIR+"/ssh_config"))
            errors += trial("hg update && python setup.py install",
                            "installing HTSQL under %s" % client_vm.name)
            errors += trial("htsql-ctl regress -qi test/regress.yaml routine",
                            "testing htsql-ctl routines")
            errors += trial("htsql-ctl regress -qi test/regress.yaml sqlite",
                            "testing sqlite backend")
            for server_vm, suite in [(pgsql84_vm, 'pgsql'),
                                     (pgsql90_vm, 'pgsql'),
                                     (pgsql91_vm, 'pgsql'),
                                     (mysql51_vm, 'mysql'),
                                     (oracle10g_vm, 'oracle'),
                                     (mssql2005_vm, 'mssql'),
                                     (mssql2008_vm, 'mssql')]:
                if server_vm.missing():
                    continue
                server_vm.start()
                username_key = "%s_USERNAME" % suite.upper()
                password_key = "%s_PASSWORD" % suite.upper()
                host_key = "%s_HOST" % suite.upper()
                port_key = "%s_PORT" % suite.upper()
                username_value = { 'pgsql': "postgres",
                                   'mysql': "root",
                                   'oracle': "system",
                                   'mssql': "sa" }[suite]
                password_value = "admin"
                host_value = "10.0.2.2"
                port_value = 10000+server_vm.port
                command = "%s=%s %s=%s %s=%s %s=%s" \
                          " htsql-ctl regress -qi test/regress.yaml %s" \
                          % (username_key, username_value,
                             password_key, password_value,
                             host_key, host_value, port_key, port_value,
                             suite)
                message = "testing %s backend against %s" \
                          % (suite, server_vm.name)
                errors += trial(command, message)
                server_vm.stop()
            client_vm.stop()
    except:
        for vm in vms:
            if vm.running():
                vm.stop()
        raise
    log()
    if errors:
        if errors == 1:
            warn("1 failed test")
        else:
            warn("%s failed tests" % errors)
    else:
        log("`All tests passed`")


