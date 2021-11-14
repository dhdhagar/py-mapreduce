from collections import defaultdict
from os import path as ospath, listdir
from pathlib import Path as pathlibpath
import pickle
import time

class Mapper:
	def __init__(self, id, n_reducers, input_partition_path, output_dir_path, udf):
		self.id = id
		self.R = n_reducers
		self.input_path = input_partition_path
		self.output_path = output_dir_path
		self.reducer_ids = [] #set()
		self.UDF = udf
		self.status = 'IDLE'  # TODO: Discuss implementation
		# Read input data
		with open(self.input_path, 'r') as reader:
			self.input_data = reader.readlines()

	def __emit_intermediate(self, emitted_kv):
		# Expected type for emitted_kv[0]: str, int, float, tuple
		key, value = emitted_kv[0], emitted_kv[1]	
		reducer_id = hash(key) % self.R
		self.intermediate_data[reducer_id][key].append(value)

	def __store_intermediate(self):
		# Create the output directory structure if it does not exist
		pathlibpath(ospath.dirname(
			f'{self.output_path}/')).mkdir(parents=True, exist_ok=True)
		# Store one intermediate file per reducer output
		for reducer in self.intermediate_data:
			self.reducer_ids.append(reducer)
			with open(f'{self.output_path}/m{self.id}r{reducer}.pickle', 'wb') as write_handle:
				pickle.dump(
					self.intermediate_data[reducer], write_handle, protocol=pickle.HIGHEST_PROTOCOL)


	def execute_map(self, master_active_reducers, update_status):
		#TODO: dynamic status update
		self.status = 'RUNNING'
		# send the status back to MASTER
		update_status.put([self.status, time.time()])
	
		self.intermediate_data = defaultdict(lambda: defaultdict(list))
		for idx, line in enumerate(self.input_data):
			# Call user-defined mapper function
			# operate upon a row in split and send status
			self.UDF(idx, line.rstrip('\n'), self.__emit_intermediate)
			update_status.put([self.status, time.time()])
			 
		self.__store_intermediate()

		self.reducer_ids.sort()
		master_active_reducers.put(self.reducer_ids) 
		self.status = 'DONE'
		#send the status back to MASTER
		update_status.put([self.status, time.time()])
