import json
import shutil
import os
import dask
import dask_awkward as dak
import awkward as ak
import numpy as np
from coffea import dataset_tools
from coffea.nanoevents import NanoEventsFactory, PFNanoAODSchema
from ndcctools.taskvine.compat import DaskVine
import fastjet
import time
import argparse
import os
import warnings
from variable_functions import *
import scipy
import sys
import cloudpickle

full_start = time.time()

def batch_copy_files(file_list, dst_folder):
    if not os.path.exists(dst_folder):
        os.makedirs(dst_folder)

    for file_name in file_list:
        if os.path.isfile(file_name):
            shutil.copy(file_name, dst_folder)
            print(f"{file_name} copied to {dst_folder}")
        else:
            print(f"file {file_name} does not exist.")

def handle_wrapper_output(wrapper_output):
    return

def preprocess_data():
    print(f"====== Preprocessing data")
    samples_path = '/project01/ndcms/jzhou24/samples'  ## Change this path to where data is
    
    filelist = {}
    categories = os.listdir(samples_path)
    print(f"categories = {categories}")

    for i in categories:
        if '.root' in os.listdir(f'{samples_path}/{i}')[0]:
            files = os.listdir(f'{samples_path}/{i}')
            filelist[i] = [f'{samples_path}/{i}/{file}' for file in files]
        else:
            sub_cats = os.listdir(f'{samples_path}/{i}')
            for j in sub_cats:
                if '.root' in os.listdir(f'{samples_path}/{i}/{j}')[0]:
                    files = os.listdir(f'{samples_path}/{i}/{j}')
                    filelist[f'{i}_{j}'] = [f'{samples_path}/{i}/{j}/{file}' for file in files]

    input_dict = {}
    for i in filelist:
        input_dict[i] = {}
        input_dict[i]['files'] = {}
        for j in filelist[i]:
            input_dict[i]['files'][j] = {'object_path': 'Events'}

    samples = input_dict

    print('doing samples')
    sample_start = time.time()

    @dask.delayed
    def sampler(samples):
        samples_ready, samples = dataset_tools.preprocess(
            samples,
            step_size=50_000, ## Change this step size to adjust the size of chunks of events
            skip_bad_files=True,
            recalculate_steps=True,
            save_form=False,
        )
        return samples_ready

    sampler_dict = {}
    for i in samples:
        sampler_dict[i] = sampler(samples[i])

    print('Compute')
    samples_postprocess = dask.compute(
        sampler_dict,
        scheduler=m.get,
        resources={"cores": 1},
        resources_mode=None,
        worker_transfers=True,
    )[0]

    samples_ready = {}
    for i in samples_postprocess:
        samples_ready[i] = samples_postprocess[i]['files']

    sample_stop = time.time()
    print('samples done')
    print('full sample time is ' + str((sample_stop - sample_start)/60))

    with open("samples_ready.json", "w") as fout:
        json.dump(samples_ready, fout)

    print(f"====== Done preprocessing data")


def analysis(events):
    dataset = events.metadata["dataset"]
    print(dataset)

    events['PFCands', 'pt'] = (
        events.PFCands.pt
        * events.PFCands.puppiWeight
    )
    
    cut_to_fix_softdrop = (ak.num(events.FatJet.constituents.pf, axis=2) > 0)
    events = events[ak.all(cut_to_fix_softdrop, axis=1)]
    
    with open('triggers.json', 'r') as f:
        triggers = json.load(f)

    trigger = ak.zeros_like(ak.firsts(events.FatJet.pt), dtype='bool')
    for t in triggers['2017']:
        if t in events.HLT.fields:
            trigger = trigger | events.HLT[t]
    trigger = ak.fill_none(trigger, False)

    events['FatJet', 'num_fatjets'] = ak.num(events.FatJet)

    goodmuon = (
        (events.Muon.pt > 10)
        & (abs(events.Muon.eta) < 2.4)
        & (events.Muon.pfRelIso04_all < 0.25)
        & events.Muon.looseId
    )

    nmuons = ak.sum(goodmuon, axis=1)
    leadingmuon = ak.firsts(events.Muon[goodmuon])

    goodelectron = (
        (events.Electron.pt > 10)
        & (abs(events.Electron.eta) < 2.5)
        & (events.Electron.cutBased >= 2) #events.Electron.LOOSE
    )
    nelectrons = ak.sum(goodelectron, axis=1)

    ntaus = ak.sum(
        (
            (events.Tau.pt > 20)
            & (abs(events.Tau.eta) < 2.3)
            & (events.Tau.rawIso < 5)
            & (events.Tau.idDeepTau2017v2p1VSjet)
            & ak.all(events.Tau.metric_table(events.Muon[goodmuon]) > 0.4, axis=2)
            & ak.all(events.Tau.metric_table(events.Electron[goodelectron]) > 0.4, axis=2)
        ),
        axis=1,
    )

    # nolepton = ((nmuons == 0) & (nelectrons == 0) & (ntaus == 0))
    onemuon = ((nmuons == 1) & (nelectrons == 0) & (ntaus == 0))

    # muonkin = ((leadingmuon.pt > 55.) & (abs(leadingmuon.eta) < 2.1))
    # muonDphiAK8 = (abs(leadingmuon.delta_phi(events.FatJet)) > 2*np.pi/3)
    
    # num_sub = ak.unflatten(num_subjets(events.FatJet, cluster_val=0.4), counts=ak.num(events.FatJet))
    # events['FatJet', 'num_subjets'] = num_sub

    # region = nolepton ## Use this option to let more data through the cuts
    region = onemuon ## Use this option to let less data through the cuts


    events['btag_count'] = ak.sum(events.Jet[(events.Jet.pt > 20) & (abs(events.Jet.eta) < 2.4)].btagDeepFlavB > 0.3040, axis=1)

    if ('hgg' in dataset) or ('hbb' in dataset):
        print("signal")
        genhiggs = events.GenPart[
            (events.GenPart.pdgId == 25)
            & events.GenPart.hasFlags(["fromHardProcess", "isLastCopy"])
        ]
        parents = events.FatJet.nearest(genhiggs, threshold=0.2)
        higgs_jets = ~ak.is_none(parents, axis=1)
        events['GenMatch_Mask'] = higgs_jets

        fatjetSelect = (
            (events.FatJet.pt > 400)
            #& (events.FatJet.pt < 1200)
            & (abs(events.FatJet.eta) < 2.4)
            & (events.FatJet.msoftdrop > 40)
            & (events.FatJet.msoftdrop < 200)
            & (region)
            & (trigger)
        )

    elif ('wqq' in dataset) or ('ww' in dataset):
        print('w background')
        genw = events.GenPart[
            (abs(events.GenPart.pdgId) == 24)
            & events.GenPart.hasFlags(['fromHardProcess', 'isLastCopy'])
        ]
        parents = events.FatJet.nearest(genw, threshold=0.2)
        w_jets = ~ak.is_none(parents, axis=1)
        events['GenMatch_Mask'] = w_jets

        fatjetSelect = (
            (events.FatJet.pt > 400)
            #& (events.FatJet.pt < 1200)
            & (abs(events.FatJet.eta) < 2.4)
            & (events.FatJet.msoftdrop > 40)
            & (events.FatJet.msoftdrop < 200)
            & (region)
            & (trigger)
        )

    elif ('zqq' in dataset) or ('zz' in dataset):
        print('z background')
        genz = events.GenPart[
            (events.GenPart.pdgId == 23)
            & events.GenPart.hasFlags(['fromHardProcess', 'isLastCopy'])
        ]
        parents = events.FatJet.nearest(genz, threshold=0.2)
        z_jets = ~ak.is_none(parents, axis=1)
        events['GenMatch_Mask'] = z_jets

        fatjetSelect = (
            (events.FatJet.pt > 400)
            #& (events.FatJet.pt < 1200)
            & (abs(events.FatJet.eta) < 2.4)
            & (events.FatJet.msoftdrop > 40)
            & (events.FatJet.msoftdrop < 200)
            & (region)
            & (trigger)
        )

    elif ('wz' in dataset):
        print('wz background')
        genwz = events.GenPart[
            ((abs(events.GenPart.pdgId) == 24)|(events.GenPart.pdgId == 23))
            & events.GenPart.hasFlags(["fromHardProcess", "isLastCopy"])
        ]
        parents = events.FatJet.nearest(genwz, threshold=0.2)
        wz_jets = ~ak.is_none(parents, axis=1)
        events['GenMatch_Mask'] = wz_jets

        fatjetSelect = (
            (events.FatJet.pt > 400)
            #& (events.FatJet.pt < 1200)
            & (abs(events.FatJet.eta) < 2.4)
            & (events.FatJet.msoftdrop > 40)
            & (events.FatJet.msoftdrop < 200)
            & (region)
            & (trigger)
        )

    else:
        print('background')
        fatjetSelect = (
            (events.FatJet.pt > 400)
            #& (events.FatJet.pt < 1200)
            & (abs(events.FatJet.eta) < 2.4)
            & (events.FatJet.msoftdrop > 40)
            & (events.FatJet.msoftdrop < 200)
            & (region)
            & (trigger)
        )
    
    events["goodjets"] = events.FatJet[fatjetSelect]
    mask = ~ak.is_none(ak.firsts(events.goodjets))
    events = events[mask]
    ecfs = {}
    
    events['goodjets', 'color_ring'] = ak.unflatten(
            color_ring(events.goodjets, cluster_val=0.4), counts=ak.num(events.goodjets)
    )

    jetdef = fastjet.JetDefinition(
        fastjet.cambridge_algorithm, 0.8
    )
    pf = ak.flatten(events.goodjets.constituents.pf, axis=1)
    cluster = fastjet.ClusterSequence(pf, jetdef)
    softdrop = cluster.exclusive_jets_softdrop_grooming()
    softdrop_cluster = fastjet.ClusterSequence(softdrop.constituents, jetdef)
    
    # (2, 3) -> (2, args.ecf_upper_bound)
    for n in range(2, args.ecf_upper_bound + 1):
        for v in range(1, int(scipy.special.binom(n, 2)) + 1):
            for b in range(5, 45, 5):
                ecf_name = f'{v}e{n}^{b/10}'
                ecfs[ecf_name] = ak.unflatten(
                    softdrop_cluster.exclusive_jets_energy_correlator(
                        func='generic', npoint=n, angles=v, beta=b/10), 
                    counts=ak.num(events.goodjets)
                )
    events["ecfs"] = ak.zip(ecfs)

    if (('hgg' in dataset) or ('hbb' in dataset) or ('wqq' in dataset) or ('ww' in dataset) or ('zqq' in dataset) or ('zz' in dataset) or ('wz' in dataset)):
        skim = ak.zip(
            {
                "Color_Ring": events.goodjets.color_ring,
                "ECFs": events.ecfs,
                "msoftdrop": events.goodjets.msoftdrop,
                "pt": events.goodjets.pt,
                "btag_ak4s": events.btag_count,
                "pn_HbbvsQCD": events.goodjets.particleNet_HbbvsQCD,
                "pn_md": events.goodjets.particleNetMD_QCD,
                "matching": events.GenMatch_Mask,
                
            },
            depth_limit=1,
        )
    else:
        skim = ak.zip(
            {
                "Color_Ring": events.goodjets.color_ring,
                "ECFs": events.ecfs,
                "msoftdrop": events.goodjets.msoftdrop,
                "pt": events.goodjets.pt,
                "btag_ak4s": events.btag_count,
                "pn_HbbvsQCD": events.goodjets.particleNet_HbbvsQCD,
                "pn_md": events.goodjets.particleNetMD_QCD,
                
            },
            depth_limit=1,
        )
       
    output_path = f"ecf_calculator_output/{dataset}/"
    os.makedirs(output_path, exist_ok=True)
    
    skim_task = dak.to_parquet(
        skim,
        output_path,
        compute=False,
    )
    return skim_task
    

def get_tasks():
    if not os.path.exists("samples_ready.json"):
        print("Error: samples_ready.json not found!")
        print("Please run the script with --preprocess parameter first to prepare the data.")
        sys.exit(1)


    with open("samples_ready.json", "r") as fin:
        samples_ready = json.load(fin)

    filtered_samples = {}
    for dataset, info in samples_ready.items():
        files = info.get("files", {})
        existing_files = {
            path: meta for path, meta in files.items() if os.path.exists(path)
        }
        if existing_files:
            filtered_samples[dataset] = {
                "files": existing_files,
                "form": info["form"],
                "metadata": info["metadata"],
            }

    if not filtered_samples:
        print("Error: no valid files found in any dataset.")
        sys.exit(1)
    
    if args.all:
        return dataset_tools.apply_to_fileset(
            analysis,
            filtered_samples,
            uproot_options={"allow_read_errors_with_report": False},
            schemaclass=PFNanoAODSchema,
        )

    if args.sub_dataset not in filtered_samples:
        print(f"Error: Sub-dataset '{args.sub_dataset}' not found or all its files are missing!")
        print("Use --show-samples to see available sub-datasets.")
        sys.exit(1)

    print("Samples ready:")
    for name, item in filtered_samples.items():
        print(name, len(item["files"]))

    print(f"Using sub-dataset: {args.sub_dataset}")
    subset = {
        args.sub_dataset: filtered_samples[args.sub_dataset]
    }

    return dataset_tools.apply_to_fileset(
        analysis,
        subset,
        uproot_options={"allow_read_errors_with_report": False},
        schemaclass=PFNanoAODSchema,
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--create', action='store_true', help='')
    parser.add_argument('--preprocess', action='store_true', help='')
    parser.add_argument('--ecf-upper-bound', type=int, choices=[3,4,5,6], default=3, help='Upper bound for ECF calculation (n=2 to n=ecf-upper-bound). Default is 3.')
    parser.add_argument('--all', action='store_true', help='Process all samples. Without this flag, only processes the specified sub-dataset')
    parser.add_argument('--show-samples', action='store_true', help='Show available samples and their file counts')
    parser.add_argument('--sub-dataset', type=str, default='hgg', help='Specify which sub-dataset to process (default: hgg)')
    parser.add_argument('--checkpoint-to', type=str, help='Save tasks to the specified file path using cloudpickle')
    parser.add_argument('--load-from', type=str, help='Load tasks from the specified file path instead of computing them')
    args = parser.parse_args()

    # Check that checkpoint_to and load_from are not used together
    if args.checkpoint_to and args.load_from:
        print("Error: --checkpoint-to and --load-from cannot be used together.")
        sys.exit(1)

    m = DaskVine(
        [9123, 9128],
        name=f"{os.environ['USER']}-hgg7",
        staging_path="/tmp/jin",
    )
    
    ## general
#    m.tune("wait-for-workers", 32)
#    m.tune("transient-error-interval", 1)
#    m.tune("kill-worker-interval-s", 300)
#    m.tune("worker-source-max-transfers", 10000)  
    
    ## fault tolerance
    # replication
    m.tune("temp-replica-count", 1)
    # checkpointing
#    m.tune("temp-file-checkpoint", 1)
#    m.tune("checkpoint-threshold", 15)

    ## scalability
    #m.tune("transfer-temps-recovery", 1)
    pruning_depth = 0

    warnings.filterwarnings("ignore", "Found duplicate branch")
    warnings.filterwarnings("ignore", "Missing cross-reference index for")
    warnings.filterwarnings("ignore", "dcut")
    warnings.filterwarnings("ignore", "Please ensure")
    warnings.filterwarnings("ignore", "invalid value")

    if args.show_samples:
        if not os.path.exists("samples_ready.json"):
            print("Error: samples_ready.json not found!")
            print("Please run the script with --preprocess parameter first to prepare the data.")
            sys.exit(1)
        with open("samples_ready.json", 'r') as fin:
            samples_ready = json.load(fin)
        print("\nAvailable samples and their file counts:")
        print("-" * 40)
        for key, value in samples_ready.items():
            print(f"{key}: {len(value['files'])} files")
        print("-" * 40)
        sys.exit(0)

    if args.preprocess:
        preprocess_data()
        sys.exit(0)

    print(f"====== Getting tasks")

    if args.load_from:
        print(f"Loading tasks from {args.load_from}")
        with open(args.load_from, 'rb') as f:
            tasks = cloudpickle.load(f)
    else:
        tasks = get_tasks()
        if args.checkpoint_to:
            print(f"Saving tasks to {args.checkpoint_to}")
            with open(args.checkpoint_to, 'wb') as f:
                cloudpickle.dump(tasks, f)

    print(f"====== Starting compute")

    computed = dask.compute(
            tasks,
            scheduler=m.get,
            resources_mode=None,
            prune_depth=pruning_depth,
            scheduling_mode="LIFO",
            worker_transfers=True,
            resources={"cores": 1},
            lib_resources={'cores': 20, 'slots': 20},
            task_mode="function-calls",
        )
   
    execution_time = round(time.time() - m.when_first_task_completed, 2)
    print(f"execution time is: {execution_time}s")  

