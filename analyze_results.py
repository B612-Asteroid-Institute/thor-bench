import google.cloud.storage as gcs
import re
import json
import dataclasses
import datetime
import yaml


DATASETS = ["full-month-small-cell"]

@dataclasses.dataclass
class BenchmarkParameters:
    instance_type: str
    thor_version: str
    dataset: str
    native_comp: bool
    use_mkl: bool

    @classmethod
    def from_gcs(cls, bucket: gcs.Bucket, dataset: str, instance_name: str):
        params_file = bucket.blob(f'{dataset}/results/{instance_name}/benchmark-parameters.json')
        params_str = params_file.download_as_string().decode('utf-8')
        params_str = params_str.replace("'", '"')
        try:
            params = json.loads(params_str)
        except json.decoder.JSONDecodeError:
            print(params_str)
            print(f'Error parsing JSON for {dataset}/{instance_name} ({params_file})')
            raise
        return cls(
            instance_type=params['instance'],
            thor_version=params['thor_version'],
            dataset=params['dataset'],
            native_comp=params['native_comp'],
            use_mkl=params['use_mkl']
        )


def get_execution_time(bucket: gcs.Bucket, dataset: str, instance_name: str) -> int:
    start_time = bucket.blob(f'{dataset}/results/{instance_name}/thor-output/start_time.txt').download_as_string().decode('utf-8').strip()
    end_time = bucket.blob(f'{dataset}/results/{instance_name}/thor-output/end_time.txt').download_as_string().decode('utf-8').strip()

    # format: 'Thu Jun 29 05:58:38 UTC 2023'
    start_time_dt = datetime.datetime.strptime(start_time, '%a %b %d %H:%M:%S %Z %Y')
    end_time_dt = datetime.datetime.strptime(end_time, '%a %b %d %H:%M:%S %Z %Y')

    seconds = (end_time_dt - start_time_dt).total_seconds()
    return seconds


@dataclasses.dataclass
class THORLogOutput:
    n_obs: int
    n_clusters: int
    n_initial_orbits: int
    n_orbits: int
    n_merged_orbits: int
    n_od_iterations: int
    
    range_and_shift_time: float
    clustering_time: float
    iod_time: float
    total_od_time: float
    total_attribution_time: float
    merging_time: float

    @classmethod
    def from_lines(cls, lines: list[str]):
        # 2023-06-28 14:11:42.684 [INFO] [140025143456192] Found 11153 observations. (main.py, rangeAndShift, 385)'
        # 2023-06-28 14:11:42.684 [INFO] [140025143456192] Range and shift completed in 28.289 seconds. (main.py, rangeAndShift, 386)
        # 2023-06-28 14:12:47.288 [INFO] [140025143456192] Found 1405 clusters. (main.py, clusterAndLink, 719)
        # 2023-06-28 14:12:47.288 [INFO] [140025143456192] Clustering and restructuring completed in 63.647 seconds. (main.py, clusterAndLink, 720)
        # 2023-06-28 14:12:51.176 [INFO] [140025143456192] Found 471 initial orbits. (iod.py, initialOrbitDetermination, 795)
        # 2023-06-28 14:12:51.201 [INFO] [140025143456192] Initial orbit determination completed in 3.889 seconds. (iod.py, initialOrbitDetermination, 812)
        # 2023-06-28 14:13:20.989 [INFO] [140025143456192] Differential correction completed in 29.712 seconds. (od.py, differentialCorrection, 801)
        # 2023-06-28 14:13:21.908 [INFO] [140025143456192] Attribution completed in 0.855 seconds. (attribution.py, attributeObservations, 302)
        # 2023-06-28 14:13:22.617 [INFO] [140025143456192] Differential correction completed in 0.600 seconds. (od.py, differentialCorrection, 801)
        # 2023-06-28 14:13:23.467 [INFO] [140025143456192] Attribution completed in 0.794 seconds. (attribution.py, attributeObservations, 302)'
        # 2023-06-28 14:13:28.351 [INFO] [140025143456192] Number of attribution / differential correction iterations: 5 (attribution.py, mergeAndExtendOrbits, 529)
        # 2023-06-28 14:13:28.351 [INFO] [140025143456192] Extended and/or merged 9 orbits into 9 orbits. (attribution.py, mergeAndExtendOrbits, 534)
        # 2023-06-28 14:13:28.351 [INFO] [140025143456192] Orbit extension and merging completed in 7.311 seconds. (attribution.py, mergeAndExtendOrbits, 539)
        
        n_obs_regex = re.compile(r'Found (\d+) observations.')
        n_clusters_regex = re.compile(r'Found (\d+) clusters.')
        n_initial_orbits_regex = re.compile(r'Found (\d+) initial orbits.')
        n_orbits_regex = re.compile(r'Extended and/or merged (\d+) orbits into (\d+) orbits.')
        n_od_iterations_regex = re.compile(r'Number of attribution / differential correction iterations: (\d+)')
        
        range_and_shift_regex = re.compile(r'Range and shift completed in (\d+\.\d+) seconds.')
        clustering_regex = re.compile(r'Clustering and restructuring completed in (\d+\.\d+) seconds.')
        iod_regex = re.compile(r'Initial orbit determination completed in (\d+\.\d+) seconds.')
        od_regex = re.compile(r'Differential correction completed in (\d+\.\d+) seconds.')
        attribution_regex = re.compile(r'Attribution completed in (\d+\.\d+) seconds.')
        merging_regex = re.compile(r'Orbit extension and merging completed in (\d+\.\d+) seconds.')

        matches = {
            "total_od_time": 0,
            "total_attribution_time": 0,
        }
        for line in lines:
            if n_obs_regex.search(line):
                matches['n_obs'] = int(n_obs_regex.search(line).group(1))
            elif n_clusters_regex.search(line):
                matches['n_clusters'] = int(n_clusters_regex.search(line).group(1))
            elif n_initial_orbits_regex.search(line):
                matches['n_initial_orbits'] = int(n_initial_orbits_regex.search(line).group(1))
            elif n_orbits_regex.search(line):
                matches['n_orbits'] = int(n_orbits_regex.search(line).group(1))
                matches['n_merged_orbits'] = int(n_orbits_regex.search(line).group(2))
            elif n_od_iterations_regex.search(line):
                matches['n_od_iterations'] = int(n_od_iterations_regex.search(line).group(1)) or 1
            elif range_and_shift_regex.search(line):
                matches['range_and_shift_time'] = float(range_and_shift_regex.search(line).group(1))
            elif clustering_regex.search(line):
                matches['clustering_time'] = float(clustering_regex.search(line).group(1))
            elif iod_regex.search(line):
                matches['iod_time'] = float(iod_regex.search(line).group(1))
            elif od_regex.search(line):
                matches['total_od_time'] += float(od_regex.search(line).group(1))
            elif attribution_regex.search(line):
                matches['total_attribution_time'] += float(attribution_regex.search(line).group(1))
            elif merging_regex.search(line):
                matches['merging_time'] = float(merging_regex.search(line).group(1))

        return cls(**matches)


@dataclasses.dataclass
class THORConfig:
    cell_area: float
    backend: str
    cluster_min_obs: int
    cluster_algorithm: str

    @classmethod
    def from_yaml(cls, bucket: gcs.Bucket, dataset: str, instance_name: str):
        config = yaml.safe_load(bucket.blob(f'{dataset}/results/{instance_name}/thor-output/thor/config.yml').download_as_string())
        return THORConfig(
            cell_area=config["RANGE_SHIFT_CONFIG"]["cell_area"],
            backend=config["RANGE_SHIFT_CONFIG"]["backend"],
            cluster_min_obs=config["CLUSTER_LINK_CONFIG"]["min_obs"],
            cluster_algorithm=config["CLUSTER_LINK_CONFIG"]["alg"],
        )
        

def analyze_thor_output_logs(bucket: gcs.Bucket, dataset: str, instance_name: str):
    logs = bucket.blob(f'{dataset}/results/{instance_name}/thor-output/thor/orbit_00000000/thor.log').download_as_string()
    lines = logs.decode("utf8").split("\n")
    return THORLogOutput.from_lines(lines)

def analyze_results(dataset: str, instance_name: str):
    client = gcs.Client()
    bucket = client.get_bucket('thor-benchmark-data')

    params = BenchmarkParameters.from_gcs(bucket, dataset, instance_name)
    execution_time = get_execution_time(bucket, dataset, instance_name)
    log_output = analyze_thor_output_logs(bucket, dataset, instance_name)
    thor_config = THORConfig.from_yaml(bucket, dataset, instance_name)


@dataclasses.dataclass
class OutputLine:
    instance_name: str
    instance_type: str
    thor_version: str
    native_comp: bool
    use_mkl: bool
    dataset: str
    cell_area: float
    backend: str
    cluster_min_obs: int
    cluster_algorithm: str
    n_obs: int
    n_clusters: int
    n_initial_orbits: int
    n_orbits: int
    n_merged_orbits: int
    n_od_iterations: int
    execution_time: float
    range_and_shift_time: float
    clustering_time: float
    iod_time: float
    total_od_time: float
    total_attribution_time: float
    merging_time: float

    @classmethod
    def from_gcs(cls, bucket: gcs.Bucket, dataset: str, instance_name: str):
        params = BenchmarkParameters.from_gcs(bucket, dataset, instance_name)
        execution_time = get_execution_time(bucket, dataset, instance_name)
        log_output = analyze_thor_output_logs(bucket, dataset, instance_name)
        thor_config = THORConfig.from_yaml(bucket, dataset, instance_name)
        return cls(
            instance_name=instance_name.split("-")[-1],
            instance_type=params.instance_type,
            thor_version=params.thor_version,
            native_comp=params.native_comp,
            use_mkl=params.use_mkl,
            dataset=dataset,
            cell_area=thor_config.cell_area,
            backend=thor_config.backend,
            cluster_min_obs=thor_config.cluster_min_obs,
            cluster_algorithm=thor_config.cluster_algorithm,
            n_obs=log_output.n_obs,
            n_clusters=log_output.n_clusters,
            n_initial_orbits=log_output.n_initial_orbits,
            n_orbits=log_output.n_orbits,
            n_merged_orbits=log_output.n_merged_orbits,
            n_od_iterations=log_output.n_od_iterations,
            execution_time=execution_time,
            range_and_shift_time=log_output.range_and_shift_time,
            clustering_time=log_output.clustering_time,
            iod_time=log_output.iod_time,
            total_od_time=log_output.total_od_time,
            total_attribution_time=log_output.total_attribution_time,
            merging_time=log_output.merging_time,
        )

    @classmethod
    def header(cls):
        return ",".join([f.name for f in  dataclasses.fields(cls)])

    def to_tsv(self):
        return ",".join([str(x) for x in dataclasses.astuple(self)])


def all_results():
    client = gcs.Client()
    bucket = client.get_bucket('thor-benchmark-data')

    datasets = gcs_subdirs(bucket, "")
    for dataset in datasets:
        instances = gcs_subdirs(bucket, dataset + "/results/")
        for instance in instances:
            yield OutputLine.from_gcs(bucket, dataset, instance)

def gcs_subdirs(bucket: gcs.Bucket, prefix: str):
    blobs = bucket.list_blobs(prefix=prefix, delimiter="/")
    # no-op to force the iterator to evaluate
    list(blobs)
    for p in blobs.prefixes:
        # trim off input prefix and trailing slash
        yield p[len(prefix):-1]



def main():
    print(OutputLine.header())
    for line in all_results():
        print(line.to_tsv())


if __name__ == "__main__":
    main()
