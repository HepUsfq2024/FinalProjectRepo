
# Final Project Instructions

**Note**: these instructions are evolving continuously.  Please check in here often.

The project consists of two phases. The first one is the development phase where 
you will ensure that the different parts of the analysis run standalone. 
You will run these parts separately over only around 100 events or so. Later, 
in the second phase of the project, you will integrate all the parts so they can run on a cluster, 
like K8s, with a single push of a button over a larger number of events.

## Phase 1: Development

### Producing your own nanoAOD files

Although nanoAOD files are part of the 2016 official release of CMS Open Data, it is very likely that in a real analysis you will need
to produce your own nanoAOD files.  We will exercise this step in our analysis chain.  Please follow the instructions below to produce these
files:

* To work with 2016 data you will need to create a new, 2016 container. The docker command is similar to what used before, however, you now have
to use the image [cmsopendata/cmssw_10_6_30-slc7_amd64_gcc700](https://opendata.cern.ch/docs/cms-guide-docker).  Also, instead of mounting the `cms_open_data_work` directory, please create and mount a directory called `cms_open_data_work2024hep`.
* In order to download the [nanoAOD producer code](https://opendata.cern.ch/record/12504), follow the instructions at [https://github.com/cms-opendata-analyses/PFNanoProducerTool](https://github.com/cms-opendata-analyses/PFNanoProducerTool).
* The idea is that you will need to produce nanoAOD files for all the datasets that you will need for your analysis.  Save those output root files (nanoAOD files)
with appropiate naming.  Remember, you will run your analysis over these files later.
* Note that there are standard config files (e.g., `pfnano_data_2016UL_OpenData.py`) dedicated, separately, for Data and MC simulations.  Adjust the inputs and outputs of the standard config files, according to the needs of your project.
* Make sure that you have selected the right datasets for your analysis.  Double check
with the instructor.
* As it is instructed [here](https://opendata.cern.ch/record/14220) you need to run over only validated data.  This means adding the following lines to
the data config file (the MC config does not need this, of course):

  ```
  import FWCore.PythonUtilities.LumiList as LumiList
  goodJSON = 'Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt'
  myLumis = LumiList.LumiList(filename = goodJSON).getCMSSWString().split(',')
  ```

  and these other ones, after the `process.source` module:

  ```
  process.source.lumisToProcess = cms.untracked.VLuminosityBlockRange()
  process.source.lumisToProcess.extend(myLumis)
  ```

* Make sure you download the file `Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt` to your working area.  One way to accomplish this is to do

  `wget https://opendata.cern.ch/record/14220/files/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt`

* For this phase (development) just run over 100 events.
* Make sure that you open the resulting `root` files and explore them in order to check that they have been filled and are correct.
* Optional: you can be creative and semi-automatize this.  For instance, instead of changing the input file each time, you can pass an argument with a short, descriptive name of the dataset and choose your testing file accordingly.  See for instance [this snippet](https://github.com/ekauffma/produce-nanoAODs/blob/57bcefe888501c502d7d0a1abc7659071e3c7b64/data_cfg.py#L33C1-L37C93), where the user passes a file index, like [these ones](https://opendata.cern.ch/record/30546#files-box-react-app), instead of a single file.
* Finally, notice that for 2016, CMS released data in the nanoAOD format already.  So these files are available in the CERN Open Data Portal.

### Running a Coffea analysis

* To work with 2016 it is probably best to create a fresh python
container with the latest official [python docker image](https://opendata.cern.ch/docs/cms-guide-docker). The docker command is similar to what used before using the image `gitlab-registry.cern.ch/cms-cloud/python-vnc:latest`.  Also, instead of mounting the `cms_open_data_python` directory, please create and mount a directory called `cms_open_data_python2024hep`.
* Don't forget to install (in the container) the packages that we will use (as described in [former tutorials](https://cms-opendata-workshop.github.io/workshop2023-lesson-ttbarljetsanalysis/)):

  ```
  pip install vector hist mplhep coffea cabinetry
  ```
* For the template we have used these datasets:
  * `/SingleMuon/Run2016H-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD`, aliased `SingleMuon` (data)
  * `/TT4b_TuneCP5_13TeV_madgraph_pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v2/NANOAODSIM` aliased `tttt` (signal, even though it does not correspond to the 4top production) or as `ttbar` (background).  Here, since this is just a template, it does not matter which process we use.  Obviously, the results will not make much sense, but the operational part should.
  * `/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM`, aliased `dyjets` (background)
  * `/WJetsToLNu_012JetsNLO_34JetsLO_EWNLOcorr_13TeV-sherpa/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM` aliased `wjets` (background)

* Remember that, if necessary, one can check the number of events in a file using the [edmEventSize](https://cms-opendata-workshop.github.io/workshop2023-lesson-cmssw/02-installation/index.html#finding-the-eventsize-of-a-root-edm-file) script from CMSSW.