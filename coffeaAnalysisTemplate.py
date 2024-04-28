import asyncio
import logging
import os
import time

import pickle
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

## input files per process, set to e.g. 10 (smaller number = faster)
##-1 means use them all
N_FILES_MAX_PER_SAMPLE = 1

## BENCHMARKING-SPECIFIC SETTINGS

## chunk size to use
CHUNKSIZE = 500_000

## metadata to propagate through to metrics
CORES_PER_WORKER = 2  # does not do anything, only used for metric gathering (set to 2 for distributed coffea-casa)

## scaling for local setups with FuturesExecutor
NUM_CORES = 4

##NanoAOD datasets are stored in data/ntuples_nanoaod.json folder. 
##This json file contains information about the number of events, 
##process and systematic. The following function reads the 
##json file and returns a dictionary with the process to run.
#--------------------------------------------------
def construct_fileset(n_files_max_per_sample,
                      dataset="SingleMuon",
                      onlyNominal=False,
                      ntuples_json=NTUPLES):
    ## Cross sections are in pb
    ## These numbers have been artificially manipulated
    ## to make the example plot coincide
    ## Xsections need to be correct and the backgrounds
    ## properly normalized
    xsec_info = {
    #    "ttbar": 831., 
    #    "wjets": 61526, 
    #    "tttt" : 0.009, 
    #    "dyjets": 6025,
        "ttbar": 831./200., 
        "wjets": 61526/80000., 
        "tttt" : 0.009, 
        "dyjets": 6025/10000.,
        "data": None
    }

    ## list of files
    with open(ntuples_json) as f:
        file_info = json.load(f)
    
    ## process into "fileset" summarizing all info
    fileset = {}
    for process in file_info.keys():
        if process == "data":
            file_list = file_info[process][dataset]["files"]
            if n_files_max_per_sample != -1:
                file_list = file_list[:n_files_max_per_sample]  # use partial set of samples

            file_paths = [f["path"] for f in file_list]
            metadata = {"process": "data", "xsec": 1}
            fileset.update({"data": {"files": file_paths, "metadata": metadata}})
            
        ##these "variations" are used for systematic studies
        ##A simple example would use only "nominal"
        for variation in file_info[process].keys():
            if onlyNominal & ~variation.startswith("nominal"): continue
            #print(variation)
            file_list = file_info[process][variation]["files"]
            if n_files_max_per_sample != -1:
                file_list = file_list[:n_files_max_per_sample] #use partial set

            file_paths = [f["path"] for f in file_list]
            nevts_total = sum([f["nevts"] for f in file_list])
            metadata = {"process": process, "variation": variation, "nevts": nevts_total, "xsec": xsec_info[process]}
            fileset.update({f"{process}__{variation}": {"files": file_paths, "metadata": metadata}})

    return fileset
#--------------------------------------------------    



##-------------Build the filesets
fileset = construct_fileset(N_FILES_MAX_PER_SAMPLE, dataset=DATA,
                            onlyNominal=True, ntuples_json=NTUPLES) 
##informational printouts
print(fileset["ttbar__nominal"]["metadata"])
print(fileset["tttt__nominal"]["metadata"])
print(fileset["wjets__nominal"]["metadata"])
print(fileset["dyjets__nominal"]["metadata"])
print(fileset["data"]["metadata"])
print(f"\nExample information in fileset:\n{{\n  'files': [{fileset['data']['files'][:]}]\n")
##----------------------------------------------------------


##---------------------------------------------------------
## This part is useful to check the total number of
## data events.  We will need to scale things properly later
## Load the JSON file
with open(NTUPLES, 'r') as file:
    data = json.load(file)
    #print(type(data))

## Initialize a variable to store the total number of events
total_events = 0

## Loop through the files in the JSON data
for file_info in data['data']['SingleMuon']['files']:
    file_path = file_info['path']
    #print(file_path)

    ## Open the ROOT file using uproot
    with uproot.open(file_path) as f:
        ## Access the 'events' TTree and count the number of entries (events)
        num_events = f['Events'].num_entries

        ## Print the file path and number of events
        print("Real data dataset info:")
        print(f"File: {file_path}, Number of Events: {num_events}")

        ## Add the number of events to the total
        total_events += num_events

## Print the total number of events
print(f"Total Number of Events: {total_events}\n")
#-----------------------------------

##------------------------------------------------------Analyzer
##Here is the main analyzer. Uses coffea/awkward to make the analysis.
class TemplateAnalysis(processor.ProcessorABC):
    def __init__(self, DATASET):
        self.DATASET = DATASET
        # booking histograms
        # define categories
        # Take a look at 
        # https://cms-opendata-workshop.github.io/workshop2023-lesson-ttbarljetsanalysis/03-coffea-analysis/index.html#histogramming-and-plotting
        process_cat = hist.axis.StrCategory([], name="process", label="Process", growth=True)
        variation_cat  = hist.axis.StrCategory([], name="variation", label="Systematic variation", growth=True)
        ## define bins (axis)
        pt_axis = hist.axis.Regular( bins=500, start=0, stop=500, name="var")
        eta_axis = hist.axis.Regular( bins=40, start=-5, stop=5, name="var")
        num_axis = hist.axis.Regular( bins=20, start=0, stop=20, name="var")      
        
        # define a dictionary of histograms
        # here is an example of a few variables that one might want to save for later and produce pretty plots
        # this, and most of the stuff here, will have to be tailored to the specifics of each analysis 
        self.hist_muon_dict = {
            'muon_pt'  : (hist.Hist(pt_axis, process_cat, variation_cat, storage=hist.storage.Weight())),
            'muon_eta' : (hist.Hist(eta_axis, process_cat, variation_cat, storage=hist.storage.Weight())),
            'nmuons'   : (hist.Hist(num_axis, process_cat, variation_cat, storage=hist.storage.Weight())),
            'jets_pt'  : (hist.Hist(pt_axis, process_cat, variation_cat, storage=hist.storage.Weight())),
            'jets_eta' : (hist.Hist(eta_axis, process_cat, variation_cat, storage=hist.storage.Weight())),
            'njets'    : (hist.Hist(num_axis, process_cat, variation_cat, storage=hist.storage.Weight())), 
            'nbjets'   : (hist.Hist(num_axis, process_cat, variation_cat, storage=hist.storage.Weight()))
        }
        
        # I think this might have to go
        # In principle sumw comes from ROOT's sumw(sum weight) function, but I do not know what it does here.
        sumw_dict = {'sumw': processor.defaultdict_accumulator(float)
        }
         
        
        ### define vector lists for scatter plot
        self.njets_signal_data = []
        self.njets_background_data = []
        self.nbjets_signal_data = []
        self.nbjets_background_data = []
        self.njets_values=[]
        self.nbjets_values=[]
        self.njets_data = []
        self.nbjets_data = []

    #------This process function is the one that
    # is run when the object of this class are forced to "run"
    def process(self, events):
        hists = self.hist_muon_dict.copy()
        # this refers to the type of dataset.  Do not confuse the process variable
        # here, with the name of the function:
        process = events.metadata["process"]
        #print(events.fields)

        #print(f'Working on process: {process}')
        #print(f'The dataset is {events.metadata["dataset"]}')
  
        if process != "data":
            # normalization for MC
            x_sec = events.metadata["xsec"]
            nevts_total = events.metadata["nevts"]
            # the luminosity has to be calculated and scaled appropiately
            # this number is hardcoded here
            lumi = 2256.38 # /pb integrated luminosity
            xsec_weight = x_sec * lumi / nevts_total #L*cross-section/N
        else:
            xsec_weight = 1


        #------------------Event Selection
        # Filtering of the data can be done essentially at two levels:
        # at the event level and the physical object level
        # Here are a few lines (mostly commented out) showcasing how one
        # could apply event selection requirements.
        # There could be a lot more event selection cuts that need to 
        # be applied, depending on the analysis
        
        #Require that the primary vertex in the event is good
        primary_vertex= events.PV.npvsGood == True
        
        ## Requirement on number of primary vertex.
        ## Require at least one
        #primary_vertex=events.PV.npvs >= 1
        
        ##Trigger selection        
        #event_filters = ( events.HLT.IsoMu20 == 1 )
        
        #update event_filters container
        #event_filters = event_filters & primary_vertex
        event_filters=primary_vertex
        selected_events = events[event_filters]
        #--------------------------------------------------------------------------


        ##---------------------------------------Object selection
        ## This is selection that applies to specific physics objects, like muons, jets, b-jets, etc.
        ## Here are a few examples, mostly commented out for the sake of getting some statistics
        ## from a very low number of events.

        ## Note the use of masks in order to apply certain requirements
        #muon_is_global= events.Muon.isGlobal == True
        #muon_is_tracker= events.Muon.isTracker == True
        
        ## Note that this could be all replaced by Tight, Medium or Loose flags, that should be
        ## operative in 2016 nanoado production
        ## we have reduced the requirements just to get more events
        ## The selection, however, needs to align to what the papers describe for the corresponding analysis
        #loose_muon_selection = (events.Muon.pt > 10) & (abs(events.Muon.eta)<2.5) \
        #                        & ((muon_is_global) | (muon_is_tracker)) \
        #                        & (events.Muon.pfRelIso04_all < 0.25)
        # selected_muon_selection = (events.Muon.pt > 26) & (abs(events.Muon.eta)<2.1) \
        #                             & ((muon_is_global) & (muon_is_tracker)) \
        #                             & (events.Muon.nTrackerLayers > 5) & (events.Muon.nStations > 0) \
        #                             & (abs(events.Muon.dxy) < 0.2) & (abs(events.Muon.dz) < 0.5) \
        #                             & (events.Muon.pfRelIso04_all < .15)
        ## Note that the selection is done using the masks above
        ## This is how filtering is done in the industry as well
        #selected_muons = events.Muon[( loose_muon_selection & selected_muon_selection)]
        #veto_muons = events.Muon[( loose_muon_selection & ~selected_muon_selection)]
        selected_muons = events.Muon[(events.Muon.pt > 5)]
        selected_muon = (ak.count(selected_muons.pt, axis=1) == 1 ) 

        
        ## Selection of jets
        #jet_selection = (events.Jet.pt > 30) & (abs(events.Jet.eta) < 2.5) & (events.Jet.jetId > 1)
        #selected_jets = events.Jet[jet_selection]
        ## Note here that some functions and tools are already implemented as part of coffea, like the TLorentzVector's nearest()
        ## See: https://github.com/CoffeaTeam/coffea/blob/d3beaff974025aa260efb2df9e8da7138a77b795/src/coffea/nanoevents/methods/vector.py#L779
        ## and the coffea documentation
        #nearest_lepton = selected_jets.nearest(selected_muons, threshold=.4)
        #selected_jets = selected_jets[ ~ak.is_none(nearest_lepton) ]
        ## the results of these 2 lines should be equivalent to the 2 lines above
        #lepton_mask = ak.any(selected_jets.metric_table(selected_lepton, metric=lambda j, e: ak.local_index(j, axis=1) == e.jetIdx,), axis=2)
        #selected_jets = selected_jets[~lepton_mask]
        ##this is an example of how b-jets might be selected
        #selected_bjets = events.Jet[jet_selection & ~ak.is_none(nearest_lepton) & (events.Jet.btagCSVV2 >=0.8)]
        #selected_jets_nobjets = events.Jet[jet_selection & ~ak.is_none(nearest_lepton) & ~(events.Jet.btagCSVV2 >=0.8)]
        selected_jets = events.Jet[(events.Jet.pt > 5)]     
        selected_bjets = events.Jet[(events.Jet.btagCSVV2 >=0.8)]
        
        
        ## Electron selection
        ##Veto electrons 
        #veto_electron_selection = (events.Electron.pt > 15) & (abs(events.Electron.eta) < 2.5) & (events.Electron.cutBased == 1)    
        ##tight electrons
        #selected_electron_selection = (events.Electron.pt > 30) & (abs(events.Electron.eta) < 2.1) & (events.Electron.cutBased == 4)
        #selected_electrons = events.Electron[ selected_electron_selection & veto_electron_selection]
        #veto_electrons = events.Electron[ veto_electron_selection ]
        selected_electrons = events.Electron[(events.Electron.pt > 3)]        
        
        ## Additional selection
        ## Exactly zero additional loose muons
        #veto_muon = (ak.count(veto_muons.pt, axis=1 ) == 0 )
        #event_filters = event_filters & veto_muon
        ##Exactly zero veto electrons
        #veto_electron = (ak.count(veto_electrons.pt, axis=1) == 0 )      
        #event_filters = event_filters & veto_electron
        ## At least 6 jets
        #at_least_one_jet = (ak.count(selected_jets.pt, axis=1) >= 6)
        #event_filters = event_filters & at_least_one_jet
        ## At least 2 bjets
        #at_least_two_bjets = (ak.count(selected_bjets.pt, axis=1) >= 2)        
        #event_filters = event_filters & at_least_two_bjets
        #print(event_filters)
        
        ##overwrite the event filter just to get
        ##more statistics
        event_filters = selected_muon
        selected_events = events[event_filters]
        selected_muons = selected_muons[event_filters]
        selected_jets = selected_jets[event_filters]
        selected_bjets = selected_bjets[event_filters]
        selected_electrons = selected_electrons[event_filters]
               
        
        ##filling of the histograms with weights       
        for ivar in [ "pt", "eta" ]:
            hists[f'muon_{ivar}'].fill(
                        var=ak.flatten(getattr(selected_muons, ivar)), process=process, variation="nominal", weight=xsec_weight)
            hists[f'jets_{ivar}'].fill(
                        var=ak.flatten(getattr(selected_jets, ivar)), process=process, variation="nominal", weight=xsec_weight)
            hists['nmuons'].fill(var=ak.count(selected_muons.pt, axis=1), process=process, variation="nominal", weight=xsec_weight)
            hists['njets'].fill(var=ak.count(selected_jets.pt, axis=1), process=process, variation="nominal", weight=xsec_weight)
            hists['nbjets'].fill(var=ak.count(selected_bjets.pt, axis=1), process=process,variation="nominal", weight=xsec_weight)
            
            njets_values = ak.count(selected_jets.pt, axis=1)
            nbjets_values=ak.count(selected_bjets.pt, axis=1)
            
            if process == "tttt":
                self.njets_signal_data.extend(njets_values)
                self.nbjets_signal_data.extend(nbjets_values)
                
            elif process == "ttbar" or process == "wjets" or process == "dyjets":
                self.njets_background_data.extend(njets_values)
                self.nbjets_background_data.extend(nbjets_values)
                
            elif process == "data":
                self.njets_data.extend(njets_values)
                self.nbjets_data.extend(nbjets_values)
                
            output = {"nevents": {events.metadata["dataset"]: len(selected_events)}, "hists" : hists,
            "njets_signal_data": self.njets_signal_data,
            "njets_background_data": self.njets_background_data,
            "nbjets_signal_data": self.nbjets_signal_data,
            "nbjets_background_data": self.nbjets_background_data, 
            "njets_data": self.njets_data,
            "nbjets_data": self.nbjets_data
            }
            
        return output

    def postprocess(self, accumulator):        
        return accumulator
#--------------------------------------------    

#--------------------------------------------
# Run the executor
# The iterative executor is a local, simple executor
# The FuturesExecutor manages threads in a more efficient way
#executor = processor.FuturesExecutor()
executor = processor.IterativeExecutor()

run = processor.Runner(executor=executor, schema=NanoAODSchema, 
                       savemetrics=True, metadata_cache={}, chunksize=CHUNKSIZE)
t0 = time.monotonic()
all_histograms, metrics = run(fileset, "Events", processor_instance=TemplateAnalysis(DATASET=DATA))
exec_time = time.monotonic() - t0
#--------------------------------------------
    

#-------------------------- --------------------------------------------
# Now, we extract the data that we will later use
nevents_info = all_histograms["nevents"]
for dataset, num_events in nevents_info.items():
    print(f"Dataset: {dataset}, Number of Events: {num_events}")
njsig = all_histograms["njets_signal_data"]
njbkg = all_histograms["njets_background_data"]
nbjsig = all_histograms["nbjets_signal_data"]
nbjbkg = all_histograms["nbjets_background_data"]
njdata = all_histograms["njets_data"]
nbjdata = all_histograms["nbjets_data"]

#save histograms in pkl file
with open("histograms.pkl", "wb") as f: 
    pickle.dump(all_histograms["hists"], f, protocol=pickle.HIGHEST_PROTOCOL)

#this is just bookeeping
dataset_source = "/data" if fileset["ttbar__nominal"]["files"][0].startswith("/data") else "other"
metrics.update({"walltime": exec_time, "num_workers": NUM_CORES, "dataset_source": dataset_source, 
                "n_files_max_per_sample": N_FILES_MAX_PER_SAMPLE, 
                "cores_per_worker": CORES_PER_WORKER, "chunksize": CHUNKSIZE})
print(f"event rate per worker (full execution time divided by NUM_CORES={NUM_CORES}): {metrics['entries'] / NUM_CORES / exec_time / 1_000:.2f} kHz")
print(f"event rate per worker (pure processtime): {metrics['entries'] / metrics['processtime'] / 1_000:.2f} kHz")
print(f"amount of data read: {metrics['bytesread']/1000**2:.2f} MB")  # likely buggy: https://github.com/CoffeaTeam/coffea/issues/717
