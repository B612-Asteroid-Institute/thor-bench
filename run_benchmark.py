import argparse
import create_instance
import ssh_instance
import random
import string

PROJECT = "moeyens-thor-dev"
ZONE = "us-west1-b"

def parse_args():
    parser = argparse.ArgumentParser(description='Benchmarking script for "thor" on Google Cloud Instances')

    parser.add_argument('-i', '--instance', type=str, required=True,
                        help='The instance class to launch')
    
    parser.add_argument('-t', '--thor-version', type=str, required=True,
                        help='The version of thor to install (as a git SHA)')
    
    parser.add_argument('-d', '--dataset', type=str, required=True,
                        help='The specific dataset inputs to provide')

    parser.add_argument("-u", "--username", type=str, required=True,
                        help="The username to use for the SSH key")
    parser.add_argument("--no-cleanup", action="store_false", dest="cleanup",
                        help="Do not delete the instance after running")
                        
    args = parser.parse_args()
    return args


def rand_str(length):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))


def main():
    args = parse_args()

    name = f"benchmark-thor-{args.thor_version[:10]}-{rand_str(5)}"
    
    instance = create_instance.create_instance(
        project_id=PROJECT,
        zone=ZONE,
        instance_name=name,
        disks=[
            create_instance.disk_from_image(
                disk_type=f"zones/{ZONE}/diskTypes/pd-standard",
                disk_size_gb=100,
                boot=True,
                source_image="projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20230616",
            ),
        ],
        external_access=True,
        machine_type=args.instance,
    )
    try:
        print("connecting to instance")
        ssh = ssh_instance.SSH(PROJECT, name, ZONE)
        ssh.wait_for_connection()
        ssh.execute_command("sudo add-apt-repository -y http://us-west1.gce.archive.ubuntu.com/ubuntu")
        ssh.execute_command("sudo apt-get update -y")
        ssh.execute_command("sudo apt-get install -y git python3-pip python3 gfortran liblapack-dev")
        ssh.execute_command("sudo chmod 777 /opt")
        ssh.execute_command("git clone https://github.com/moeyensj/thor.git /opt/thor")
        ssh.execute_command("git clone https://github.com/oorb/oorb.git /opt/oorb")
        ssh.execute_command("pip install numpy")
        ssh.execute_command("sudo ln -sf ~/.local/bin/f2py /usr/bin/f2py")
        ssh.execute_command("cd /opt/oorb && ./configure gfortran opt --with-pyoorb --with-f2py=/usr/bin/f2py --with-python=python3")
        ssh.execute_command("sudo pip install -v /opt/oorb")
        ssh.execute_command("cd /opt/oorb && make ephem")
        ssh.execute_command("cd /opt/thor && git checkout {}".format(args.thor_version))
        ssh.execute_command("cd /opt/thor && sudo pip install -v .")
        ssh.execute_command("export OORB_DATA=/opt/oorb/data && python3 /opt/thor/runTHOR.py --help")
        

    except Exception as e:
        print(e)
    finally:
        if args.cleanup:
            create_instance.delete_instance("moeyens-thor-dev", "us-west1-b", name)

    
if __name__ == "__main__":
    main()
