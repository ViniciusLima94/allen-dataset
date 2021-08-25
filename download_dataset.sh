#!/bin/bash

#SBATCH -J download_dataset   # Job name
#SBATCH -o dd.out             # Name of stdout output file (%j expands to %jobID)
#SBATCH -t 700:00:00          # Run time (hh:mm:ss) - 1.5 hours
#SBATCH -N 1

python3 -O download_dataset.py 1
