from collections import defaultdict
from os import path as ospath, listdir
from pathlib import Path as pathlibpath
import pickle
import itertools
import time

class Reducer:
	def __init__(self, id, n_mappers, intermediate_dir_path, output_dir_path, udf):
		self.id = id
		self.M = n_mappers
		self.intermediate_dir_path = intermediate_dir_path
		self.output_path = output_dir_path
		self.UDF = udf
		self.status = 'IDLE'

		# Read input data
		self.input_data = []

		for mapper in range(self.M):
			with open( f'{self.intermediate_dir_path}/m{mapper}r{self.id}.pickle', "rb" ) as intmd_file:
				self.input_data.append(pickle.load(intmd_file))
		# Creating a dictionary containing all keys for this reducer id
		self.kv_data = {k:list(itertools.chain(
			                *[[0] if v is None else v for v in 
			                [d.get(k) for d in self.input_data]])) 
		                    for k in {k for d in self.input_data for k in d}}

	def __emit_final(self, emitted_k, emitted_v=None):
		"""This will write final output to disk"""
		#print(f"Creating file {self.output_path}/{self.id}.txt")
		pathlibpath(ospath.dirname(
			f'{self.output_path}/')).mkdir(parents=True, exist_ok=True)
		out_file = open(f'{self.output_path}/{self.id}.txt', 'a+')
		# print (f'{self.output_path}/{self.id}.txt')
		output_line = f"{emitted_k}"
		if emitted_v is not None:
			output_line += f": {emitted_v}"
		output_line += "\n"
		out_file.write(output_line)
		out_file.close()

	def delete_final(self):
		print(f"Deleting file {self.output_path}/{self.id}.txt")
		if pathlibpath(f'{self.output_path}/{self.id}.txt').exists():
			pathlibpath(f'{self.output_path}/{self.id}.txt').unlink()

	def execute_reduce(self, update_status):
		self.status = 'RUNNING'
		# send the status back to MASTER
		update_status.put([self.status, time.time()])

		for k in self.kv_data:
			#operate upon a row in split and send status 
			out = self.UDF(k, self.kv_data[k], self.__emit_final)
			update_status.put([self.status, time.time()])

		self.status = 'DONE'
		# send the status back to MASTER
		update_status.put([self.status, time.time()])
		# update_status.close() #close pipe connection

