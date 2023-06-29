import argparse
import json
import create_instance
import ssh_instance
import random
import string
import google.cloud.storage

PROJECT = "moeyens-thor-dev"
ZONE = "us-central1-a"


def parse_args():
    parser = argparse.ArgumentParser(
        description='Benchmarking script for "thor" on Google Cloud Instances'
    )

    parser.add_argument(
        "-i", "--instance", type=str, required=True, help="The instance class to launch"
    )

    parser.add_argument(
        "-t",
        "--thor-version",
        type=str,
        required=True,
        help="The version of thor to install (as a git SHA)",
    )

    parser.add_argument(
        "-d",
        "--dataset",
        type=str,
        required=True,
        help="The specific dataset inputs to provide",
    )

    parser.add_argument(
        "-u",
        "--username",
        type=str,
        required=True,
        help="The username to use for the SSH key",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_false",
        dest="cleanup",
        help="Do not delete the instance after running",
    )

    parser.add_argument(
        "--native-comp",
        action="store_true",
        dest="native_comp",
        help="Compile a tuned binary for THOR and its dependencies",
    )

    parser.add_argument(
        "--use-mkl",
        action="store_true",
        dest="use_mkl",
        help="Use Intel MKL for linear algebra operations",
    )

    args = parser.parse_args()
    return args


def rand_str(length):
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))


def install_python(ssh):
    ssh.execute_command("sudo apt-get install -y python3-pip python3")


def apt_repo(zone):
    # eg us-central1-a becomes http://us-central1.gce.archive.ubuntu.com/ubuntu"
    region, _ = zone.rsplit("-", 1)
    return "http://{}.gce.archive.ubuntu.com/ubuntu".format(region)


def install_mkl(ssh):
    # Install Intel MKL
    ssh.execute_command(
        "wget -O- https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB \
| gpg --dearmor | sudo tee /usr/share/keyrings/oneapi-archive-keyring.gpg > /dev/null"
    )
    ssh.execute_command(
        'echo "deb [signed-by=/usr/share/keyrings/oneapi-archive-keyring.gpg] https://apt.repos.intel.com/oneapi all main" | sudo tee /etc/apt/sources.list.d/oneAPI.list'
    )
    ssh.execute_command("sudo apt-get update -y")
    ssh.execute_command("sudo apt-get install -y intel-oneapi-mkl")


def install_numpy(ssh, native_comp=False):
    if not native_comp:
        ssh.execute_command("sudo pip install numpy==1.24")
        return

    # Install numpy from source
    ssh.execute_command("sudo apt-get install -y gfortran liblapack-dev")
    ssh.execute_command("git clone https://github.com/numpy/numpy.git /opt/numpy")
    ssh.execute_command("sudo pip install cython")
    ssh.execute_command(
        "cd /opt/numpy && git checkout v1.24.4 && git submodule update --init"
    )
    ssh.execute_command(
        "cd /opt/numpy && sudo python3 setup.py build --cpu-baseline=native install"
    )


def install_openorb(ssh, native_comp=False):
    ssh.execute_command("sudo apt-get install -y gfortran liblapack-dev")
    ssh.execute_command("git clone https://github.com/oorb/oorb.git /opt/oorb")
    ssh.execute_command(
        "cd /opt/oorb && ./configure gfortran opt --with-pyoorb --with-f2py=/usr/local/bin/f2py --with-python=python3"
    )
    if native_comp:
        # Add '-march=native' to compiler options by running a sed
        # script directly on the Makefile.includse file. This is a
        # hack to get around the fact that the configure script
        # doesn't support this option.
        ssh.execute_command(
            "sed -i 's/FCOPTIONS = .*/FCOPTIONS = $(FCOPTIONS_OPT_GFORTRAN) -march=native/g' /opt/oorb/Makefile.include"
        )
    ssh.execute_command("sudo pip install -v setuptools wheel")

    # --no-build-isolation is needed because we need to ensure we use
    # the same version of numpy as the one we compiled previously so
    # that it matches the version of f2py we passed in to ./configure.
    ssh.execute_command("sudo pip install --no-build-isolation -v /opt/oorb")

    ssh.execute_command("cd /opt/oorb && sudo make ephem")


def install_thor(ssh: ssh_instance.SSH, thor_version: str, arm: bool=False):
    if arm:
        # Need to build healpy from source since no wheel is available.
        ssh.execute_command("sudo apt-get install -y pkg-config libcfitsio-dev")
    
    ssh.execute_command("git clone https://github.com/moeyensj/thor.git /opt/thor")
    ssh.execute_command("cd /opt/thor && git checkout {}".format(thor_version))
    ssh.execute_command("cd /opt/thor && sudo pip install -v .")


def enable_sysstat(ssh, interval_seconds=1, count=60):
    ssh.execute_command("sudo apt-get install -y sysstat")
    ssh.execute_command("echo 'ENABLED=\"true\"' | sudo tee /etc/default/sysstat")
    ssh.execute_command(
        "echo '* * * * * root /usr/lib/sysstat/sa1 10 6' | sudo tee /etc/cron.d/sysstat"
    )
    ssh.execute_command("sudo systemctl restart sysstat")


def load_dataset(ssh, dataset):
    ssh.execute_command("mkdir /opt/thor-data /opt/thor-output")
    ssh.execute_command(
        f"gsutil cp gs://thor-benchmark-data/{dataset}/config.yaml /opt/thor-data/config.yaml"
    )
    ssh.execute_command(
        f"gsutil cp gs://thor-benchmark-data/{dataset}/observations.csv /opt/thor-data/observations.csv"
    )
    ssh.execute_command(
        f"gsutil cp gs://thor-benchmark-data/{dataset}/orbits.csv /opt/thor-data/orbits.csv"
    )


def main():
    args = parse_args()

    name = f"benchmark-thor-{args.instance}-{args.thor_version[:6]}-{rand_str(4)}"

    if args.instance.startswith("t2a"):
        # ARM
        image = "projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-arm64-v20230616"
    else:
        # x86
        image = "projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20230616"

    instance = create_instance.create_instance(
        project_id=PROJECT,
        zone=ZONE,
        instance_name=name,
        service_account=create_instance.service_account(
            "thor-benchmarker@moeyens-thor-dev.iam.gserviceaccount.com",
            ["https://www.googleapis.com/auth/cloud-platform"],
        ),
        disks=[
            create_instance.disk_from_image(
                disk_type=f"zones/{ZONE}/diskTypes/pd-balanced",
                disk_size_gb=100,
                boot=True,
                source_image=image,
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
        if not args.instance.startswith("t2a"):
            ssh.execute_command(f"sudo add-apt-repository -y {apt_repo(ZONE)}",)
        else:
            # ARM
            pass
        ssh.execute_command("sudo apt-get update -y")
        ssh.execute_command("sudo apt-get install -y git")
        ssh.execute_command("sudo chmod 777 /opt")

        install_python(ssh)
        if args.use_mkl:
            install_mkl(ssh)
        install_numpy(ssh, native_comp=args.native_comp)
        install_openorb(ssh, native_comp=args.native_comp)
        install_thor(ssh, args.thor_version, arm=args.instance.startswith("t2a"))

        enable_sysstat(ssh)

        # Download data
        load_dataset(ssh, args.dataset)

        # Note the time
        ssh.execute_command("date > /opt/thor-output/start_time.txt")

        # Run THOR
        ssh.execute_command(
            "export OORB_DATA=/opt/oorb/data && python3 /opt/thor/runTHOR.py --config /opt/thor-data/config.yaml /opt/thor-data/observations.csv /opt/thor-data/orbits.csv /opt/thor-output/thor/"
        )

        # Note the time
        ssh.execute_command("date > /opt/thor-output/end_time.txt")

        # Collect system resource data
        ssh.execute_command("sudo cp /var/log/sysstat/sa* /opt/thor-output/")

        # Copy output to GCS
        ssh.execute_command(
            f"gsutil cp -r /opt/thor-output gs://thor-benchmark-data/{args.dataset}/results/{name}/"
        )

        google.cloud.storage.Client().bucket("thor-benchmark-data").blob(
            f"{args.dataset}/results/{name}/benchmark-parameters.json"
        ).upload_from_string(json.dumps(vars(args)))

        print("all done!")
        print(f"results are in gs://thor-benchmark-data/{args.dataset}/results/{name}")
        print(
            f"download command: \n\tgsutil cp -r gs://thor-benchmark-data/{args.dataset}/results/{name}/ ."
        )

    except Exception as e:
        print(e)
    finally:
        if args.cleanup:
            create_instance.delete_instance(PROJECT, ZONE, name)


if __name__ == "__main__":
    main()
