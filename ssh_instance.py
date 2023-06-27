import subprocess
import sys
import time

class SSH:
    def __init__(self, project, instance, zone):
        self.project = project
        self.instance = instance
        self.zone = zone

    def execute_command(self, command):
        args = [
            "gcloud", "compute", "ssh",
            f"--project={self.project}",
            f"--zone={self.zone}",
            self.instance,
            "--command", command
        ]
        print(f"running command: {' '.join(args)}")
        proc = subprocess.run(args, stdout=sys.stdout, stderr=sys.stderr, stdin=sys.stdin)
        if proc.returncode != 0:
            raise Exception(f"Error while executing command.")

    def wait_for_connection(self):
        print("attempting ssh connection...")
        args = [
            "gcloud", "compute", "ssh",
            f"--project={self.project}",
            f"--zone={self.zone}",
            self.instance,
            "--command", "echo 'connected!'"
        ]
        i = 0
        while i < 10:
            if i >= 1:
                print("retrying...")
            proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if proc.returncode == 0:
                print("connected!")
                return
            time.sleep(5)
            i += 1
        raise Exception(f"Unable to connect to instance: {proc.stderr.decode('utf-8')}")
            
            

