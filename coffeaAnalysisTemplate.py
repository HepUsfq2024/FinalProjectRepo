import asyncio
import logging
import os
import time

import vector; vector.register_awkward() 
import awkward as ak
from coffea import processor
from coffea.nanoevents import transforms
from coffea.nanoevents.methods import base, vector
from coffea.nanoevents import NanoAODSchema
import hist
import json
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import uproot

import pandas as pd


DATA = "SingleMuon"
NTUPLES = "data/ntuples.json"

# input files per process, set to e.g. 10 (smaller number = faster)
#-1 means use them all
N_FILES_MAX_PER_SAMPLE = 1

### BENCHMARKING-SPECIFIC SETTINGS

# chunk size to use
CHUNKSIZE = 500_000

# metadata to propagate through to metrics
CORES_PER_WORKER = 2  # does not do anything, only used for metric gathering (set to 2 for distributed coffea-casa)

# scaling for local setups with FuturesExecutor
NUM_CORES = 4

#NanoAOD datasets are stored in data/ntuples_nanoaod.json folder. 
#This json file contains information about the number of events, 
#process and systematic. The following function reads the 
#json file and returns a dictionary with the process to run.
#--------------------------------------------------
def construct_fileset(n_files_max_per_sample,
                      dataset="SingleMuon",
                      onlyNominal=False,
                      ntuples_json=NTUPLES):
    # using https://atlas-groupdata.web.cern.ch/atlas-groupdata/dev/AnalysisTop/TopDataPreparation/XSection-MC15-13TeV.data
    # for reference
    # Cross sections are in pb
    xsec_info = {
        "ttbar": 831., 
        "wjets": 61526, 
        "tttt" : 0.009, 
        "dyjets": 6025,
        "data": None
    }

    # list of files
    with open(ntuples_json) as f:
        file_info = json.load(f)
    
      # process into "fileset" summarizing all info
    fileset = {}
    for process in file_info.keys():
        if process == "data":
            file_list = file_info[process][dataset]["files"]
            if n_files_max_per_sample != -1:
                file_list = file_list[:n_files_max_per_sample]  # use partial set of samples
                #file_list = file_list[:]  # use all of samples

            file_paths = [f["path"] for f in file_list]
            metadata = {"process": "data", "xsec": 1}
            fileset.update({"data": {"files": file_paths, "metadata": metadata}})
            
        #these "variations" are used for systematic studies
        #A simple example would use only "nominal"
        for variation in file_info[process].keys():
            if onlyNominal & ~variation.startswith("nominal"): continue
            #print(variation)
            file_list = file_info[process][variation]["files"]
            if n_files_max_per_sample != -1:
                file_list = file_list[:n_files_max_per_sample] #use partial set
                #file_list = file_list[:]

            file_paths = [f["path"] for f in file_list]
            nevts_total = sum([f["nevts"] for f in file_list])
            metadata = {"process": process, "variation": variation, "nevts": nevts_total, "xsec": xsec_info[process]}
            fileset.update({f"{process}__{variation}": {"files": file_paths, "metadata": metadata}})

    return fileset
#--------------------------------------------------    



#-------------Build the filesets
fileset = construct_fileset(N_FILES_MAX_PER_SAMPLE, dataset=DATA,
                            onlyNominal=True, ntuples_json=NTUPLES) 
#informational printouts
print(fileset["ttbar__nominal"]["metadata"])
print(fileset["tttt__nominal"]["metadata"])
print(fileset["wjets__nominal"]["metadata"])
print(fileset["dyjets__nominal"]["metadata"])
print(fileset["data"]["metadata"])
print(f"\ndata information in fileset:\n{{\n  'files': [{fileset['data']['files'][:]}, ...],")
#----------------------------------------------------------


#---------------------------------------------------------
# This part is useful to check the total number of
# data events.  We will need to scale things properly later
# Load the JSON file
# with open(NTUPLES, 'r') as file:
#     data = json.load(file)

# # Initialize a variable to store the total number of events
# total_events = 0

# # Loop through the files in the JSON data
# for file_info in data['data']['SingleMuon']['files']:
#     file_path = file_info['path']

#     # Open the ROOT file using uproot
#     with uproot.open(file_path) as f:
#         # Access the 'events' TTree and count the number of entries (events)
#         num_events = f['Events'].num_entries

#         # Print the file path and number of events
#         #print(f"File: {file_path}, Number of Events: {num_events}")

#         # Add the number of events to the total
#         total_events += num_events

# # Print the total number of events
# print(f"\nTotal Number of Events: {total_events}")
#-----------------------------------

#Analyzer
#Here is the main analyzer. Uses coffea/awkward to make the analysis.
#Advice: to understand how the selection is working, 
#print the different arrays before and after the selections are made.
class fourTopAnalysis(processor.ProcessorABC):
    def __init__(self, DATASET):
        self.DATASET = DATASET
        ### booking histograms
        ## define categories
        process_cat = hist.axis.StrCategory([], name="process", label="Process", growth=True)
        variation_cat  = hist.axis.StrCategory([], name="variation", label="Systematic variation", growth=True)
        ## define bins (axis)
        pt_axis = hist.axis.Regular( bins=500, start=0, stop=500, name="var")
        eta_axis = hist.axis.Regular( bins=40, start=-5, stop=5, name="var")
        num_axis = hist.axis.Regular( bins=20, start=0, stop=20, name="var")
        #Htb
        htb_axis=hist.axis.Regular( bins=100, start=0, stop=1000, name="var")
        #Htratio
        htrat_axis=hist.axis.Regular(bins=500,start=0, stop=1, name="var")
        #3rd-highest CSV
        csv_axis=hist.axis.Regular(bins=100,start=0, stop=1, name="var")      
        
        ## define a dictionary of histograms
        self.hist_muon_dict = {
            'muon_pt'  : (hist.Hist(pt_axis, process_cat, variation_cat, storage=hist.storage.Weight())),
            'muon_eta' : (hist.Hist(eta_axis, process_cat, variation_cat, storage=hist.storage.Weight())),
            'nmuons'   : (hist.Hist(num_axis, process_cat, variation_cat, storage=hist.storage.Weight())),
            'jets_pt'  : (hist.Hist(pt_axis, process_cat, variation_cat, storage=hist.storage.Weight())),
            'jets_eta' : (hist.Hist(eta_axis, process_cat, variation_cat, storage=hist.storage.Weight())),
            'njets'    : (hist.Hist(num_axis, process_cat, variation_cat, storage=hist.storage.Weight())), 
            'nbjets'   : (hist.Hist(num_axis, process_cat, variation_cat, storage=hist.storage.Weight())),
            'htb'      : (hist.Hist(htb_axis, process_cat, variation_cat, storage=hist.storage.Weight())), #variable for bdt
            'htrat'    : (hist.Hist(htrat_axis, process_cat, variation_cat, storage=hist.storage.Weight())), #variable for bdt
            'third_highest_csv': (hist.Hist(csv_axis, process_cat, variation_cat, storage=hist.storage.Weight())) #variable for bdt

        }
        
        sumw_dict = {'sumw': processor.defaultdict_accumulator(float)
        }
        
        # Variables para contar el flujo de cortes
        self.cut_flow_counters = {
            "All Events": 0,
            "Primary Vertex": 0,
            "Trigger (IsoMu20)": 0
        }
        
        
        ### define vectors for scatter plot
        self.njets_signal_data = []
        self.njets_background_data = []
        self.nbjets_signal_data = []
        self.nbjets_background_data = []
        self.htb_signal_data = []
        self.htb_background_data = []
        self.htrat_signal_data = []
        self.htrat_background_data = []
        
        self.njets_values=[]
        self.nbjets_values=[]
        self.htb_values=[]
        self.htrat_values=[]
        
        self.njets_data = []
        self.nbjets_data = []
        self.htb_data = []
        self.htrat_data = []

    def process(self, events):

        hists = self.hist_muon_dict.copy()

        process = events.metadata["process"]  # "ttbar" etc.

    
        if process != "data":
            # normalization for MC
            x_sec = events.metadata["xsec"]
            nevts_total = events.metadata["nevts"]
            lumi = 2256.38 # /pb integrated luminosity
            xsec_weight = x_sec * lumi / nevts_total #L*cross-section/N
        else:
            xsec_weight = 1

        events["pt_nominal"] = 1.0

        ### OBJECT SELECTION
        
        ### Object selection: Muon (Tight - muon id definition in nanoAOD does not work, have to define manual)
        
        muon_is_global= events.Muon.isGlobal == True
        muon_is_tracker= events.Muon.isTracker == True
        
        loose_muon_selection = (events.Muon.pt > 10) & (abs(events.Muon.eta)<2.5) \
                                & ((muon_is_global) | (muon_is_tracker)) \
                                & (events.Muon.pfRelIso04_all < 0.25)
        selected_muon_selection = (events.Muon.pt > 26) & (abs(events.Muon.eta)<2.1) \
                                    & ((muon_is_global) & (muon_is_tracker)) \
                                    & (events.Muon.nTrackerLayers > 5) & (events.Muon.nStations > 0) \
                                    & (abs(events.Muon.dxy) < 0.2) & (abs(events.Muon.dz) < 0.5) \
                                    & (events.Muon.pfRelIso04_all < .15)
        #selected_muon_s
        selected_muons = events.Muon[( loose_muon_selection & selected_muon_selection)]
        veto_muons = events.Muon[( loose_muon_selection & ~selected_muon_selection)]
        
        ### Object selection: Jets
        
        jet_selection = (events.Jet.pt * events["pt_nominal"] > 30) & (abs(events.Jet.eta) < 2.5) & (events.Jet.jetId > 1)
        selected_jets = events.Jet[jet_selection]
        nearest_lepton = selected_jets.nearest(selected_muons, threshold=.4)
        selected_jets = selected_jets[ ~ak.is_none(nearest_lepton) ]
        
        ## the results of these 2 lines should be equivalent to the 2 lines above
        #lepton_mask = ak.any(selected_jets.metric_table(selected_lepton, metric=lambda j, e: ak.local_index(j, axis=1) == e.jetIdx,), axis=2)
        #selected_jets = selected_jets[~lepton_mask]
        
        selected_bjets = events.Jet[jet_selection & ~ak.is_none(nearest_lepton) & (events.Jet.btagCSVV2 >=0.8)]
        selected_jets_nobjets = events.Jet[jet_selection & ~ak.is_none(nearest_lepton) & ~(events.Jet.btagCSVV2 >=0.8)]  ### this we might use it later
        
        
        ### Object selection: Electron
        
        #Veto electrons 
        veto_electron_selection = (events.Electron.pt > 15) & (abs(events.Electron.eta) < 2.5) & (events.Electron.cutBased == 1)
        
        #tight electrons
        selected_electron_selection = (events.Electron.pt > 30) & (abs(events.Electron.eta) < 2.1) & (events.Electron.cutBased == 4)
        
        selected_electrons = events.Electron[ selected_electron_selection & veto_electron_selection]
        veto_electrons = events.Electron[ veto_electron_selection ]
        
        
       ################
        #### Event Selection
        ################
        
        #self.cut_flow_counters["All Events"][process] += len(events)
        #primary_vertex= events.PV.npvsGood == True
        
        #event_filters=primary_vertex
        #selected_events = events[event_filters]
        #self.cut_flow_counters["Primary Vertex"][process] += len(selected_events)
        
        event_filters = ( events.HLT.IsoMu20 == 1 ) 
       # selected_events = events[event_filters]
        
       # self.cut_flow_counters["Trigger (IsoMu20)"][process] += len(selected_events)
        
        #number of primary vertex, at least one
        #FIRST CUT
        #primary_vertex=events.PV.npvs >= 1
        
        #event_filters = ( events.HLT.IsoMu20 == 1 )   #trigger selection (1 value per event)
        
        #event_filters = event_filters & primary_vertex
        
        selected_muon = (ak.count(selected_muons.pt, axis=1) == 1 ) 
        
        event_filters = event_filters & selected_muon
        
        #Exactly zero additional loose muons
        veto_muon = (ak.count(veto_muons.pt, axis=1 ) == 0 )
        event_filters = event_filters & veto_muon
        
        #Exactly zero veto electrons
        veto_electron = (ak.count(veto_electrons.pt, axis=1) == 0 )      
        event_filters = event_filters & veto_electron
        
        # At least 6 jets
        at_least_one_jet = (ak.count(selected_jets.pt, axis=1) >= 6)
        event_filters = event_filters & at_least_one_jet
        
        # At least 2 bjets
        at_least_two_bjets = (ak.count(selected_bjets.pt, axis=1) >= 2)        
        event_filters = event_filters & at_least_two_bjets
        #print(event_filters)
        
        # apply event filters
        selected_events = events[event_filters]
        selected_muons = selected_muons[event_filters]
        selected_jets = selected_jets[event_filters]
        selected_bjets = selected_bjets[event_filters]
        selected_electrons = selected_electrons[event_filters]
               
        ##### VARIABLES FOR BDT ####
        
        
        #### Calculate HTb
        htb = ak.sum(selected_bjets.pt, axis=1)  
        
        #### Calculate H_t^ratio
        selected_jets_sorted = ak.sort(selected_jets.pt, axis=1, ascending=False)
               
        third_highest_csv=0.0
        htrat=[]
        
        if len(ak.fields(selected_jets_sorted)) >= 4: 
            four_leading_jets=selected_jets_sorted[:, :4]
            ht_leading_jets=ak.sum(four_leading_jets,axis=1)
            other_jets=selected_jets_sorted[:, 4:]
            ht_other_jets = ak.sum(other_jets,axis=1)
            htrat = ht_other_jets/ht_leading_jets
        
        #### Calculate Third-highest CSV
        
        #Sort jets by CSV values 
        sorted_jets= ak.argsort(selected_jets.btagCSVV2, axis=1)
        
        #Extract the third-highest CSV value
        if len(ak.fields(sorted_jets)) >= 2:
            third_highest_csv = sorted_jets[:, 2]
               
        for ivar in [ "pt", "eta" ]:
            
            hists[f'muon_{ivar}'].fill(
                        var=ak.flatten(getattr(selected_muons, ivar)), process=process, variation="nominal", weight=xsec_weight)
            hists[f'jets_{ivar}'].fill(
                        var=ak.flatten(getattr(selected_jets, ivar)), process=process, variation="nominal", weight=xsec_weight)
            hists['nmuons'].fill(var=ak.count(selected_muons.pt, axis=1), process=process, variation="nominal", weight=xsec_weight)
            hists['njets'].fill(var=ak.count(selected_jets.pt, axis=1), process=process, variation="nominal", weight=xsec_weight)
            hists['nbjets'].fill(var=ak.count(selected_bjets.pt, axis=1), process=process,variation="nominal", weight=xsec_weight)
            hists['htb'].fill(var=htb, process=process, variation="nominal", weight=xsec_weight)
            hists['htrat'].fill(var=htrat, process=process, variation="nominal", weight=xsec_weight)
            hists['third_highest_csv'].fill(var=third_highest_csv, process=process, variation="nominal", weight=xsec_weight)
            
            njets_values = ak.count(selected_jets.pt, axis=1)
            nbjets_values=ak.count(selected_bjets.pt, axis=1)
            htrat_values=htrat
            htb_values=htb
            
            if process == "tttt":
                self.njets_signal_data.extend(njets_values)
                self.nbjets_signal_data.extend(nbjets_values)
                self.htb_signal_data.extend(htb)
                self.htrat_signal_data.extend(htrat)
                
            elif process == "ttbar" or process == "wjets" or process == "dyjets":
                self.njets_background_data.extend(njets_values)
                self.nbjets_background_data.extend(nbjets_values)
                self.htb_background_data.extend(htb)
                self.htrat_background_data.extend(htrat)
                
            elif process == "data":
                self.njets_data.extend(njets_values)
                self.nbjets_data.extend(nbjets_values)
                self.htb_data.extend(htb)
                self.htrat_data.extend(htrat)
                
            output = {"nevents": {events.metadata["dataset"]: len(selected_events)}, "hists" : hists,

        "njets_signal_data": self.njets_signal_data,
        "njets_background_data": self.njets_background_data,
        "nbjets_signal_data": self.nbjets_signal_data,
        "nbjets_background_data": self.nbjets_background_data, 
        "htb_signal_data": self.htb_signal_data,
        "htb_background_data": self.htb_background_data,
        "htrat_signal_data":self.htrat_signal_data,
        "htrat_background_data":self.htrat_background_data,
        "njets_data": self.njets_data,
        "nbjets_data": self.nbjets_data,
        "htb_data": self.htb_data,
        "htrat_data":self.htrat_data}
            
        return output

    def postprocess(self, accumulator):
        
             
        return accumulator
    

#---------------------------
# Run the executor
#The iterative executor is a local executor
executor = processor.IterativeExecutor()

run = processor.Runner(executor=executor, schema=NanoAODSchema, 
                       savemetrics=True, metadata_cache={}, chunksize=CHUNKSIZE)
t0 = time.monotonic()
all_histograms, metrics = run(fileset, "Events", processor_instance=fourTopAnalysis(DATASET=DATA))
exec_time = time.monotonic() - t0
#--------------------------------------
    

#----------------------------------------------------------------------
# Now, we extract the data that we will later use
nevents_info = all_histograms["nevents"]
for dataset, num_events in nevents_info.items():
    print(f"Dataset: {dataset}, Number of Events: {num_events}")
njsig = all_histograms["njets_signal_data"]
njbkg = all_histograms["njets_background_data"]

nbjsig = all_histograms["nbjets_signal_data"]
nbjbkg = all_histograms["nbjets_background_data"]

htbsig = all_histograms["htb_signal_data"]
htbbkg = all_histograms["htb_background_data"]

htratsig = all_histograms["htrat_signal_data"]
htratbkg = all_histograms["htrat_background_data"]

njdata = all_histograms["njets_data"]
nbjdata = all_histograms["nbjets_data"]
htbdata= all_histograms["htb_data"]
htratdata = all_histograms["htrat_data"]

# %store njsig
# %store njbkg
# %store nbjsig 
# %store nbjbkg
# %store htbsig 
# %store htbbkg 
# %store htratsig
# %store htratbkg 

# %store njdata 
# %store nbjdata
# %store htbdata
# %store htratdata


import pickle

with open("histograms.pkl", "wb") as f: 
    pickle.dump(all_histograms["hists"], f, protocol=pickle.HIGHEST_PROTOCOL)

dataset_source = "/data" if fileset["ttbar__nominal"]["files"][0].startswith("/data") else "https://xrootd-local.unl.edu:1094" # TODO: xcache support
metrics.update({"walltime": exec_time, "num_workers": NUM_CORES, "dataset_source": dataset_source, 
                "n_files_max_per_sample": N_FILES_MAX_PER_SAMPLE, 
                "cores_per_worker": CORES_PER_WORKER, "chunksize": CHUNKSIZE})#

print(f"event rate per worker (full execution time divided by NUM_CORES={NUM_CORES}): {metrics['entries'] / NUM_CORES / exec_time / 1_000:.2f} kHz")
print(f"event rate per worker (pure processtime): {metrics['entries'] / metrics['processtime'] / 1_000:.2f} kHz")
print(f"amount of data read: {metrics['bytesread']/1000**2:.2f} MB")  # likely buggy: https://github.com/CoffeaTeam/coffea/issues/717