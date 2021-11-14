#!/bin/bash

eval "$(conda shell.bash hook)"
conda env create -f environment.yml
conda activate mapreduce

cd test_scripts
python test_1.py
echo ----------------------------------------------------------------------------
python test_2.py
echo ----------------------------------------------------------------------------
python test_3.py
echo ----------------------------------------------------------------------------
python test_4.py