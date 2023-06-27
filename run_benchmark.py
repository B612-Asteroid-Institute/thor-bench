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

    name = f"benchmark-thor-{args.instance}-{args.thor_version[:6]}-{rand_str(4)}"

    instance = create_instance.create_instance(
        project_id=PROJECT,
        zone=ZONE,
        instance_name=name,
        service_account=create_instance.service_account(
            "thor-benchmarker@moeyens-thor-dev.iam.gserviceaccount.com",
            ["https://www.googleapis.com/auth/cloud-platform"]
        ),
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

        # Install system dependencies
        ssh.execute_command("sudo add-apt-repository -y http://us-west1.gce.archive.ubuntu.com/ubuntu")
        ssh.execute_command("sudo apt-get update -y")
        ssh.execute_command("sudo apt-get install -y git python3-pip python3 gfortran liblapack-dev sysstat")

        # Install openorb and thor
        ssh.execute_command("sudo chmod 777 /opt")
        ssh.execute_command("git clone https://github.com/moeyensj/thor.git /opt/thor")
        ssh.execute_command("git clone https://github.com/oorb/oorb.git /opt/oorb")
        ssh.execute_command("pip install numpy==1.24")
        ssh.execute_command("sudo ln -sf ~/.local/bin/f2py /usr/bin/f2py")
        ssh.execute_command("cd /opt/oorb && ./configure gfortran opt --with-pyoorb --with-f2py=/usr/bin/f2py --with-python=python3")
        ssh.execute_command("sudo pip install -v /opt/oorb")
        ssh.execute_command("pip uninstall numpy -y")
        ssh.execute_command("cd /opt/oorb && make ephem")
        ssh.execute_command("cd /opt/thor && git checkout {}".format(args.thor_version))
        ssh.execute_command("cd /opt/thor && sudo pip install -v .")
        ssh.execute_command("export OORB_DATA=/opt/oorb/data && python3 /opt/thor/runTHOR.py --help")

        # Enable sysstat to collect system resource data every 1 minute
        ssh.execute_command("echo 'ENABLED=\"true\"' | sudo tee /etc/default/sysstat")
        ssh.execute_command("echo '* * * * * root /usr/lib/sysstat/sa1 1 1' | sudo tee /etc/cron.d/sysstat")
        ssh.execute_command("sudo systemctl restart sysstat")

        # Download data
        ssh.execute_command("mkdir /opt/thor-data /opt/thor-output")
        ssh.execute_command(f"gsutil cp gs://thor-benchmark-data/{args.dataset}/config.yaml /opt/thor-data/config.yaml")
        ssh.execute_command(f"gsutil cp gs://thor-benchmark-data/{args.dataset}/observations.csv /opt/thor-data/observations.csv")
        ssh.execute_command(f"gsutil cp gs://thor-benchmark-data/{args.dataset}/orbits.csv /opt/thor-data/orbits.csv")

        # Note the time
        ssh.execute_command("date > /opt/thor-output/start_time.txt")

        # Run THOR
        ssh.execute_command("export OORB_DATA=/opt/oorb/data && python3 /opt/thor/runTHOR.py --config /opt/thor-data/config.yaml /opt/thor-data/observations.csv /opt/thor-data/orbits.csv /opt/thor-output/thor/")

        # Note the time
        ssh.execute_command("date > /opt/thor-output/end_time.txt")

        # Collect system resource data
        ssh.execute_command("sudo cp /var/log/sysstat/sa* /opt/thor-output/")

        # Copy output to GCS
        ssh.execute_command(f"gsutil cp -r /opt/thor-output gs://thor-benchmark-data/{args.dataset}/results/")

        print("all done!")
        print(f"results are in gs://thor-benchmark-data/{args.dataset}/results/{args.name}")
        print(f"download command: \n\tgsutil cp -r gs://thor-benchmark-data/{args.dataset}/results/ .")

    except Exception as e:
        print(e)
    finally:
        if args.cleanup:
            create_instance.delete_instance("moeyens-thor-dev", "us-west1-b", name)


if __name__ == "__main__":
    main()
