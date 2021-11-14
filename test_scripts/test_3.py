import sys
import os
from random import random
import math
import csv
import multiprocessing as mp
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../code")

from mapreduce import MapReduce, getConf

if __name__ == '__main__':
	mp.set_start_method("fork")
	print("""
TEST #3: K-Nearest Neighbors Search
-----------------------------------
""")
	K = 4
	# Generate a random query data point
	query_data = [random() for _ in range(10)]
	
	# Generate and write a random input dataset
	with open('data/knn-dataset.txt', 'w') as filename:
		writer = csv.writer(filename, delimiter = ',')
		for i in range(10000):
			data_point = [random() for _ in range(10)]
			writer.writerow(data_point)
	
	# Mapper 1: Calculate the l2 distance between the query and a data point
	def udf_mapper(key, value, emitter):
		split_values = list(map(float, value.split(',')))
		total = 0
		for i,v in enumerate(query_data):
			total += (v - split_values[i])**2
		distance = math.sqrt(total)
		emitter((key, (value, distance))) # key = line number for the mapper's input

	# Reducer 1: Sort the values by distance and persist only the top K
	def udf_reducer(key, values, emitter):
		result = 0
		values = sorted(values, key=lambda x: x[1])
		for v in values[:K]:
			emitter(v[0], v[1])

	# Mapper 2: Read and emit the input data values and the corresponding distance to the query point
	def udf_mapper2(key, value, emitter):
		split_values = value.split(':')
		emitter((0, (split_values[0], float(split_values[1])))) # Same key so that we can handle all the data in one udf_reducer call

	# Reducer 2: Sort the remaining values and persist the top K
	def udf_reducer2(key, values, emitter):
		values = sorted(values, key=lambda x: x[1])
		for v in values[:K]:
			emitter(v[0])

	# Execute MapReduce job
	m, r, f, kill_idx = getConf("data/config_knn.txt")
	f = "data/"+f
	mapred = MapReduce(m, r, f, udf_mapper, udf_reducer, kill_idx)
	output_path_from_previous_job = os.path.abspath(mapred.OUT_DIR)
	mapred2 = MapReduce(m, 1, output_path_from_previous_job, udf_mapper2, udf_reducer2, -1)

	# Sequential test verification	
	print("Verifying MapReduce results:")
	# Read MapReduce results
	output_arr = mapred2.read_output()
	knn_mapred = set()
	for line in output_arr:
		knn_mapred.add(line)
	# Compute sequential results
	knn_seq = set()
	with open('data/knn-dataset.txt', 'r') as reader:
		input_data = reader.readlines()
	dataset = []
	for idx, line in enumerate(input_data):
		line = line.rstrip('\n')
		line_split = line.split(',')
		line_split = list(map(float, line_split))
		dataset.append(line_split)
	distances = []
	for data in dataset:
		distance = 0
		for i in range(len(query_data)):
			distance += (query_data[i] - data[i])**2
		distance = math.sqrt(distance)
		distances.append(distance)
	sorted_idxs = [x for x,y in sorted(enumerate(distances), key = lambda x: x[1])]
	for idx in sorted_idxs[:K]:
		knn_seq.add(input_data[idx])
	for nn in knn_mapred:
		if nn not in knn_seq:
			print('FAIL')
			exit()
	print('PASS')
