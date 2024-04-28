import os
import time

import hist
import json
import matplotlib.pyplot as plt
import mplhep as hep
from hist.intervals import ratio_uncertainty
import matplotlib as mpl
import numpy as np
import pickle
from collections import OrderedDict 

#---------------------Plotting function with CMS style
def plotHisto( thehist, thebkgs, histName = "muon_pt", xlabel = "Muon $p_{T}$ [GeV]", rebinFactor = 7, 
                  xmin = 20j, xmax = 300j, mcFactor = 1, xlog=False):

    data = thehist[histName][ xmin:xmax:hist.rebin(rebinFactor), "data", "nominal"]
    hists = [] 
    tot = data.copy()
    tot.reset()

    for ibkg in thebkgs:
        hists.append( (mcFactor*thehist[histName][ xmin:xmax:hist.rebin(rebinFactor), ibkg, "nominal"]) )
        tot += (mcFactor*thehist[histName][ xmin:xmax:hist.rebin(rebinFactor), ibkg, "nominal"])
        
    #note that the signal dataset name is harcoded here
    signal = 40*thehist[histName][ xmin:xmax:hist.rebin(rebinFactor), "tttt", "nominal"]
        
    fig, (ax, rax) = plt.subplots(2, 1, gridspec_kw=dict(height_ratios=[3, 1], hspace=0.03), sharex=True)
    #the labels are harcoded and it is for 2015
    hep.cms.label("Open Data", ax=ax, data=True, lumi=2.26, year=2015) #, rlabel="2.3 $\mathrm{fb^{-1}}$, 2015 (8 TeV)")
    plt.style.use(hep.style.CMS)

    hep.histplot(data, ax=ax, histtype='errorbar', color='k', capsize=4, yerr=True, label="Data")
    hep.histplot(hists, ax=ax, stack=True, histtype='fill', color=bkgs_colors, label=bkgs)
    hep.histplot(signal, ax=ax, histtype='step', color='black', label="$ \\bar{t}t \\bar{t} (x20) $")

    errps = {'hatch':'////', 'facecolor':'none', 'lw': 0, 'color': 'k', 'alpha': 0.4}
    ax.stairs(
        values=tot.values() + np.sqrt(tot.values()),
        baseline=tot.values() - np.sqrt(tot.values()),
        edges=data.axes[0].edges, **errps, label='Stat. unc.')
    ax.set_yscale("log")
    ax.set_ylim(0.1, 1e5)
    ax.legend()
    ax.set_ylabel(f"Events / {rebinFactor}")
    
    yerr = ratio_uncertainty(data.values(), tot.values(), 'poisson')
    rax.stairs(1+yerr[1], edges=tot.axes[0].edges, baseline=1-yerr[0], **errps)
    hep.histplot(data.values()/tot.values(), tot.axes[0].edges, yerr=np.sqrt(data.values())/tot.values(),
        ax=rax, histtype='errorbar', color='k', capsize=4, label="Data")
    
    # Set the number of y ticks
    ax.set_yticks([1e-1,1,1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8])
    
    # Set the number of x ticks
    if histName == "muon_pt":
        ax.set_xticks([20, 50, 100, 150, 200, 250, 300])
        
    if histName == "njets":
        ax.set_xticks([4,5,6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
        
    if histName == "htb":
        ax.set_xticks([100, 200, 300, 400, 500, 600, 700, 800, 900, 1000])


    rax.axhline(1, ls='--', color='k')
    ### more labels
    plt.xlabel(xlabel)
    plt.ylabel("Data/MC")
    plt.show()
#-----------------------------------


with open("histograms.pkl", "rb") as f:
    h2 = pickle.load(f)
    print(h2)
    print(h2.keys())


    ### list of bkgs to plot
    dictBkgs = OrderedDict()
    dictBkgs["ttbar"] = { "color" : "#80ff00", "label" : "$t\\bar{t}$" }
    dictBkgs["wjets"] = { "color" : "#ff9f00", "label" : "EW" }
    dictBkgs["dyjets"] = { "color" : "#007fff", "label" : "EW" }

    bkgs = list(dictBkgs.keys())[::-1]
    print(bkgs)
    bkgs_colors = [ col["color"] for i, col in dictBkgs.items() ]
    print(bkgs_colors)
    bkgs_label = [ col["label"] for i, col in dictBkgs.items() ]

    plotHisto(h2,bkgs,xlog=True)


