
"""A plugin for running a script, and retrieving some file as output


"""
import os
import posixpath
import time

from starcluster.clustersetup import DefaultClusterSetup
from starcluster.logger import log

def user_ssh(node, user, cmd):
    """run code as user, from user's home"""
    return node.ssh.execute("su - %s -c 'cd && %s'"%(user, cmd))

class ScriptSetup(DefaultClusterSetup):
    
    def __init__(self, script, output=None, runner='python', **kwargs):
        self.script = os.path.expanduser(script)
        self.output = output
        self.runner = runner
        super(ScriptSetup, self).__init__(**kwargs)
    
    def _send_script(self, node, user):
        fname = os.path.basename(self.script)
        user_home = node.getpwnam(user).pw_dir
        remotepath = posixpath.join(user_home, fname)
        log.info("Sending %s to %s as %s"%(self.script, node.alias, remotepath))
        rf = node.ssh.remote_file(remotepath)
        with open(self.script) as f:
            rf.write(f.read())
        rf.close()
        
        # give ownership to the user
        node.ssh.execute("chown %s %s"%(user,remotepath))
        return remotepath

    def _run_on_node(self, node, user):
        tic = time.time()
        rfile = self._send_script(node, user)
        s = user_ssh(node, user, "%s %s"%(self.runner, rfile))
        #for line in s:
        #    # print the output of the script, which may be meaningful
        #    log.info(line)
        output = self.output
        if output:
            matches = user_ssh(node, user, "ls %s"%output)
            user_home = node.getpwnam(user).pw_dir
            for m in matches:
                log.info("retrieving output from %s"%m)
                base = posixpath.basename(m)
                # from cwd
                if base == m:
                    src = posixpath.join(user_home, m)
                    node.ssh.sftp.get(src, m)
                else:
                    # from abspath, into cwd
                    node.ssh.sftp.get(m, base)
        
        mins = (time.time()-tic)/60.
        log.info("Running script %s on %s took %.2f mins"%(self.script, node.alias, mins))
    
    def run(self, nodes, master, user, user_shell, volumes):
        log.info("Number of nodes: %d" % len(nodes))
        log.info("nodes: %s" % ",".join(map(lambda x: x.alias, nodes)))
        for node in nodes:
            self.pool.simple_job(self._run_on_node, (node, user), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

    def on_add_node(self, new_node, nodes, master, user, user_shell, volumes):
        log.info("Running script on node %s" % new_node.alias)
        self.pool.simple_job(self._run_on_node, (new_node, user), jobid=new_node.alias)
