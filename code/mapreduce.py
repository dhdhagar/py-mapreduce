from master import Master
from os import path as ospath, listdir
from pathlib import Path as pathlibpath
from time import time
import multiprocessing as mp

def getConf(filename):
	'''Function to fetch parameters from config file'''
	'''Config File Format: # of mappers, # of reducers, output file path, kill index'''
	with open(filename, "r") as confFile:
	    for line in confFile:
	        conf_args = line.split(",")
	        m = int(conf_args[0].strip())
	        r = int(conf_args[1].strip())
	        f = conf_args[2].strip()
	        kill_idx = int(conf_args[3].strip())
	        return m, r, f, kill_idx

class MapReduce:
	def __get_input_splits(self, input_path):
		# Internal function to optionally partition (if single file passed) or retrieve a list of partitioned input files
		if isinstance(input_path, str):
			if ospath.isdir(input_path):
				input_files = [f'{input_path}/{f}' for f in listdir(input_path)]
				n = len(input_files)
				if n == 0:
					raise ValueError(
						"Expected 1 or more files in the directory")
				elif n < self.M:
					print(f"Insufficient input files. Setting mappers to {n}")
					self.M = n
					# TODO: Can be changed to split all files in the directory evenly for each mapper
			elif ospath.isfile(input_path):
				SPLIT_DIR = f"{self.TMP_DIR}/input"
				# Create input_splits directory
				pathlibpath(ospath.dirname(
					f'{SPLIT_DIR}/')).mkdir(parents=True, exist_ok=True)
				with open(input_path, 'r') as reader:
					mappers_used = set()
					n = 0
					line = reader.readline()
					while len(line) != 0:
						if not line.endswith('\n'):
							line += '\n'
						mapper_id = n % self.M
						mappers_used.add(mapper_id)
						with open(f'{SPLIT_DIR}/{mapper_id}.txt', 'a') as writer:
							writer.write(line)
						line = reader.readline()
						n += 1
				if len(mappers_used) < self.M:
					print(
						f"Insufficient input lines. Setting mappers to {mappers_used}")
					self.M = mappers_used
				input_files = [
					f'{SPLIT_DIR}/{i}.txt' for i in range(self.M)]
			else:
				raise ValueError("Expected a file or directory path")
		else:
			raise ValueError(
				"Type mismatch. Expected a file or directory path")
		return input_files

	def __init__(self, n_mappers, n_reducers, input_path, map_func, reduce_func, kill_idx=-1):
		self.job_id = f'{int(time())}'
		if ospath.isdir(f'./tmp/{self.job_id}'):
			i = 1
			while ospath.isdir(f'./tmp/{self.job_id}-{i}'):
				i += 1
			self.job_id += f'-{i}'
		#get the number of cpu cores
		n_processes = mp.cpu_count()
		#adjust num_mappers and num_reducers according to num_cpu_cores
		self.M, self.R = min(n_mappers, n_processes), min(n_reducers, n_processes)
		print(f"Starting Job ID #{self.job_id} (M={self.M}, R={self.R})\n")
		# Create a temp directory for intermediate files
		self.TMP_DIR = f'./tmp/{self.job_id}'
		# Create an output directory for final reducer files
		self.OUT_DIR = f'./output/{self.job_id}'
		pathlibpath(ospath.dirname(
			f'{self.TMP_DIR}/')).mkdir(parents=True, exist_ok=True)
		# Get the list of input file paths; split data into intermediate files, if required
		input_files = self.__get_input_splits(input_path)
		# Initialize master
		self.master = Master(self.M, self.R, input_files, self.TMP_DIR, self.OUT_DIR)
		# Transfer control to master and execute MapReduce
		self.master.execute(map_func, reduce_func, kill_idx=kill_idx)
		# Prints output directory and returns the path
		print("\n{} output file(s) written to {}/\n".format(self.R, ospath.abspath(self.OUT_DIR)))

	def read_output(self):
		output = []
		for fname in listdir(self.OUT_DIR):
			with open(ospath.join(self.OUT_DIR, fname), 'r') as reader:
				output += reader.readlines()
		return output
