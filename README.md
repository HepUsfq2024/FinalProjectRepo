
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
* Optional: you can be creative and semi-automatize this.   For instance, instead of changing the input file each time, you can pass an argument with a short, descriptive name of the dataset and choose your testing file accordingly.  See for instance [this snippet](https://github.com/ekauffma/produce-nanoAODs/blob/57bcefe888501c502d7d0a1abc7659071e3c7b64/data_cfg.py#L33C1-L37C93), where the user passes a file index, like [these ones](https://opendata.cern.ch/record/30546#files-box-react-app), instead of a single file.
* Finally, notice that for 2016, CMS released data in the nanoAOD format already.  So these files are available in the CERN Open Data Portal.

### Preparing for the Coffea analysis

* To work with 2016 it is probably best to create a fresh python
container with the latest official [python docker image](https://opendata.cern.ch/docs/cms-guide-docker). The docker command is similar to what used before using the image `gitlab-registry.cern.ch/cms-cloud/python-vnc:latest`.  Also, instead of mounting the `cms_open_data_python` directory, please create and mount a directory called `cms_open_data_python2024hep`.
* Don't forget to install (in the container) the packages that we will use (as described in [former tutorials](https://cms-opendata-workshop.github.io/workshop2023-lesson-ttbarljetsanalysis/)):

  ```
  pip install vector hist mplhep coffea cabinetry

  ```
* As before, let's downgrade coffea:
  ```
  pip install coffea==0.7.22
  ```

* Now, create a directory called `data` in the `cms_open_data_python2024hep` (if working in the USFQ minicluster, it is probably best to do it in the local machine and change the permissions to 777 in order to avoid permission issues).
* Download the file `ntuples.json` (from the `FinalProjectRepo` in Github) **into the `data` directory** you just created (again, it is probably best to do this in the local machine).  One can do this by downloading directly the `raw` version from Github with *wget*:

  ```
  wget https://raw.githubusercontent.com/HepUsfq2024/FinalProjectRepo/main/data/ntuples.json
  ```
* Note that this `ntuples.json` file contains the datasets suited for
the template example.  The analysis template is an old version of an analysis that used to search for a *4-top* production process.  The datasets there do not really matter much, what it matters is the structure of the filesets.  You will have to replace those datasets and files with the files that you have produced in the first stage, the nano ADO production (just in case, if we need later, be open to the possibility of using nanoaod files already produced centrally by CMS).  

  There are two versions of this `ntuples.json` file in the `data` folder of the Github repository `FinalProjectRepo`, which serve as examples.  One with filepaths pointing to the cern opendata servers (uses the `root:` director) and the other one that assumes local files (uses the `file:` director.)

* For the template `ntuples.json` we have used these datasets:
  * `/SingleMuon/Run2016H-UL2016_MiniAODv2_NanoAODv9-v1/NANOAOD`, aliased `SingleMuon` (data)
  * `/TT4b_TuneCP5_13TeV_madgraph_pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v2/NANOAODSIM` aliased `tttt` (signal, even though it does not correspond to the 4top production) or as `ttbar` (background).  Here, since this is just a template, it does not matter which process we use.  Obviously, the results will not make much sense, but the operational part should.
  * `/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM`, aliased `dyjets` (background)
  * `/WJetsToLNu_012JetsNLO_34JetsLO_EWNLOcorr_13TeV-sherpa/RunIISummer20UL16NanoAODv9-106X_mcRun2_asymptotic_v17-v1/NANOAODSIM` aliased `wjets` (background)
* Note that each simulated dataset has the total number of events and the number of events that correspond to each file.  
* In the `ntuples.json` file you can add as many files as you want, but since you will only end up producing one nanoAOD file per dataset, then this template is enough.
* Remember that one can check the number of events in a file using the [edmEventSize](https://cms-opendata-workshop.github.io/workshop2023-lesson-cmssw/02-installation/index.html#finding-the-eventsize-of-a-root-edm-file) script from CMSSW (that means that you could run this script from the CMSSW container over these files; just look at the example.)  One could also use `uproot` to get this information (an example is in the coffea analysis code.)
* Also note that the `ntuples.json` file has a tag called *nominal*.  This is not going to be very important for us, but the reason for this is that for each simulated dataset one can introduce several variations for it.  E.g., one *nominal*, one *scaledup*, one *scaleddown*, etc., where some theoretical parameters are varied in the production.  These would be used to estimate a portion of the systematic uncertainties, but we are not going to worry about that here.


### Running the Coffea Analysis

* The template `coffeaAnalysisTemplate.py` is ready to be executed as an example on how to run an analysis over
all relevant datasets.  One can download this file from the repository and run it like

  ``` 
  python coffeaAnalysisTemplate.py
  ```
  This will produce a file called `histograms.pkl`, which contains all histograms for all relevant datasets, acording to whatever has been coded in the template.  This file will be later used to create beautiful plots with a different script.

* The template code `coffeaAnalysisTemplate.py` contains several comments, which hopefully facilitate the understanding of its inner workings.

* The student should make all efforts to understand this code in order to be able to be able to modify it to introduce the adecuate datasets, cross sections, the appropiate analysis cuts, etc.

* Finally, in order to create the relevant plots for the analysis, one can use the `plot.py` script that has been also included in the project repository as an example.  Once the `histograms.pkl` has been produced, one should be able to modify the `plot.py` script to create not only one (the example only creates one plot) but any plot needed for the final presentation.

* The student should understand he `plot.py` script deeply in order to make the required plots for his presentation.


### Luminosity, cross sections and normalization

* The student should find out the luminosity for the 2016 data and normalize it according to the number of events used for the `data` dataset.

* Information about the luminosity for 2016 can be found [here](https://opendata.cern.ch/record/1059).  Ask the instructor if you encounter difficulties.

* The cross section values should be read from the Analysis Note of the analysis.  If the datasets are not exatly the same,
  use the one that seems closest to the ones you end up using.

* Work out the normalization for the luminosity based on the data number of events that you end up using.




  