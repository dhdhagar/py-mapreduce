import sys
import os
from random import randint
import csv
import multiprocessing as mp
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../code")

from mapreduce import MapReduce, getConf

if __name__ == '__main__':
	mp.set_start_method("fork")
	print("""
TEST #2: Average Test Score
---------------------------
""")
	# Generate and write random input data
	with open('data/test_scores.txt', 'w') as filename:
		writer = csv.writer(filename, delimiter = ' ')
		names = ['Shantam', 'Dhruv', 'Shubham', 'John', 'David', 'Britta', 'Sergei', 'Shenoy']
		for i in range(10000):
			name_idx = randint(0, len(names) - 1)
			score = randint(0,100)
			writer.writerow([names[name_idx], score])

	# Define custom mapper function
	def udf_mapper(key, value, emitter):
		split_values = value.split(' ')
		name = split_values[0]
		if (name == 'Shubham' or name == 'Dhruv' or name == 'Shantam'):
			emitter((name, int(split_values[1])))
		
	# Define custom reducer function
	def udf_reducer(key, values, emitter):
		result = 0
		for v in values:
			result += v
		emitter(key, result/len(values))

	
	# Execute MapReduce job
	m, r, f, kill_idx = getConf("data/config_test_score.txt")
	f = "data/"+f
	mapred = MapReduce(m, r, f, udf_mapper, udf_reducer, kill_idx)

	# Sequential test verification	
	print("Verifying MapReduce results:")
	# Read MapReduce results
	output_arr = mapred.read_output()
	averages_mapred = {}
	for line in output_arr:
		name, average = line.rsplit(':', 1)
	averages_mapred[name] = float(average)
	# Compute sequential results
	averages_seq = {'Shubham': 0, 'Dhruv': 0, 'Shantam': 0}
	score_count = {'Shubham': 0, 'Dhruv': 0, 'Shantam': 0}
	with open('data/test_scores.txt', 'r') as reader:
		input_data = reader.readlines()
	for idx, line in enumerate(input_data):
		line = line.rstrip('\n')
		line_split = line.split(' ')
		if line_split[0] in ['Shubham', 'Dhruv', 'Shantam']:
			averages_seq[line_split[0]] += float(line_split[1])
			score_count[line_split[0]] += 1.0
	for i in averages_seq:
		averages_seq[i] /= score_count[i]
	for name in averages_mapred:
		if averages_mapred[name] != averages_seq[name]:
			print('FAIL')
			exit()
	print('PASS')
