from subprocess import Popen, PIPE

def inject_fault(password, target_files):

    # Start docker command and inject fault while querying.
    p = Popen(['sudo', '-p', '-S', 'ls'], stdin=PIPE, stdout=PIPE,
              stderr=PIPE, universal_newlines=True)
    p.communicate(password + '\n')


