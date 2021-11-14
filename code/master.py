from mapper import Mapper
from reducer import Reducer
import multiprocessing as mp
# import os
import time

class Master:
	def __init__(self, m, r, input_files, temp_dir, output_dir):
		self.TMP_DIR = temp_dir
		self.OUT_DIR = output_dir
		# self.M, self.R, self.input_file_paths = m, r, input_files

		self.input_file_paths = input_files
		self.cpu_count = mp.cpu_count()
		self.M = min(self.cpu_count, m)
		self.R = min(self.cpu_count, r)
		self.max_attempts = 3
		self.timeout = 3
		self.phase_flag = -1

	def restart_process(self, i, num_workers, kill_idx):
		#communicate worker failure to the user
		print (f"Worker {i+1}  of {num_workers} has crashed, spawning another process")
		#kill the process explicitly to ensure no background computation/resource allocation
		self.processes[i].kill()
		if (self.phase_flag == 0):
			self.reducer_ids[i] = mp.Queue()
			self.cs[i] = mp.Queue()
			self.processes[i] = mp.Process(target = self.mappers[i].execute_map, args = (self.reducer_ids[i], self.cs[i]))
		elif (self.phase_flag == 1):
			self.reducers[i].delete_final()
			self.cs[i] = mp.Queue()
			self.processes[i] = mp.Process(target = self.reducers[i].execute_reduce, args=(self.cs[i], ))
		# Execute Worker
		self.processes[i].start()
		#kill process if test flag is set for multiple faults
		if (kill_idx == -2):
			print(f"Killing process 1")
			self.processes[1].kill()

	def execute(self, map_func, reduce_func, kill_idx=-1):
		# Logic for coordinating mappers and reducer
		self.mappers = []
		self.reducers = []
		self.active_reducers = []

		#instantiate mappers
		for idx in range(len(self.input_file_paths)):
			self.mappers.append(Mapper(
				idx, self.R, self.input_file_paths[idx], f'{self.TMP_DIR}/intermediate', map_func))
		
		# NOTE: Keeping this for future exextuion time comparison
		# for m in mappers:
		# 	m.execute_map()
		# 	while (m.status != 'DONE'):
		# 		continue
		# 	self.active_reducers = self.active_reducers | m.reducer_ids
		# 	print('MAPPER {} finished executing'.format(m.id+1)) #, m.id, m.status)

		print("Map phase:")
		self.phase_flag = 0
		#instantiate processes for map phase
		self.processes = [None]*self.M
		self.reducer_ids = [None]*self.M
		self.ps, self.cs = [None]*self.M, [None]*self.M
		self.mapper_status = [True]*self.M
		self.attempts = [0]*self.M

		for i, m in enumerate(self.mappers):
		
			#queue used for message passing
			self.reducer_ids[i] = mp.Queue()
			# ps[i], cs[i] = mp.Pipe()
			self.cs[i] = mp.Queue()
			self.processes[i] = mp.Process(target = m.execute_map, args = (self.reducer_ids[i], self.cs[i]))
			#execute mapper
			self.processes[i].start()
			#simulate process crash to test fault tolerance
			if (kill_idx == i):
				print(f"Killing process {i}")
				self.processes[i].kill()
		
		# Code for testing fault tolerance timeout
		if (kill_idx == -2):
			print(f"Killing process 1")
			self.processes[1].kill()

		#wait until all mappers have finished
		#mapping_status: Checks if phase is complete
		mapping_status = False
		while (mapping_status == False):
			mapping_status = True
			for i, m in enumerate(self.mappers):
				curr_status = None
				while True:
					try:
						#heartbeat message
						[curr_status, timestamp] = self.cs[i].get(timeout = self.timeout)
						break
					except:
						#no message received, check if max attempts reached
						if (self.attempts[i] < self.max_attempts):
							# restart replacement worker, increment attempt count 
							self.restart_process(i, self.M, kill_idx)
							self.attempts[i] += 1
						else:
							for i, m in enumerate(self.mappers):
								self.processes[i].kill()
							raise ValueError("RETRY_ERROR: Maximum attempts reached, job failed")

			#check status received
			if curr_status == 'DONE' and self.mapper_status[i] == True:				
				self.mapper_status[i] = False
				#get all valid reducer_ids
				self.active_reducers += self.reducer_ids[i].get()
				#wait until all processes have been completed
				self.processes[i].join()
			else:
				mapping_status = False
				

		print ("\nAll mappers have finished executing")
		print("\nReduce phase:")
		self.phase_flag = 1
		# NOTE: Keeping this for future exextuion time comparison
		# for r in reducer:
		# 	r.execute_reduce()
		# 	while (r.status != 'DONE'):
		# 		continue
		# 	print('REDUCER {} finished executing'.format(r.id+1))#, r.id, r.status)
		
		#similar to map phase, instantiate all reducers and processes
		self.active_reducers = (list(set(self.active_reducers)))
		self.processes = [None]*self.R
		self.ps, self.cs = [None]*self.R, [None]*self.R
		self.reducer_status = [True]*len(self.active_reducers)
		
		for idx in (self.active_reducers):
			self.reducers.append(Reducer(
				idx, len(self.input_file_paths), f'{self.TMP_DIR}/intermediate', self.OUT_DIR, reduce_func))
		
		#setting up processes for reducers
		for i, r in enumerate(self.reducers):
			self.cs[i] = mp.Queue()
			self.processes[i] = mp.Process(target = r.execute_reduce, args=(self.cs[i], ))
			self.processes[i].start()
			#killing certain workers to test fault tolerance
			if (kill_idx == i):
				print (f"Killing process {i+1}")
				self.processes[i].kill()

		#check for heartbeat messages, similar to map phase
		reducing_status =  False
		while reducing_status == False:
			reducing_status = True
			for i, r in enumerate(self.reducers):
				curr_status = None
				while True:
					try:
						#print(self.reducer_status[i])
						if (self.reducer_status[i] is True):
							[curr_status, timestamp] = self.cs[i].get(timeout = self.timeout)
						break
					except:
						if (self.attempts[i] < self.max_attempts):
							self.restart_process(i, self.R, kill_idx)
							self.attempts[i] += 1
						else:
							print ("Max attempts reached, task not completed")
							for i, m in enumerate(self.reducers):
								self.processes[i].kill()
							raise ValueError("TIMEOUT ERROR: Max attempts reached, task not completed")

				if curr_status == 'DONE' and self.reducer_status[i] == True:
					self.reducer_status[i] = False
					self.processes[i].join()
				elif curr_status == 'RUNNING':
					reducing_status = False

		print ("\nAll reducing tasks have been completed")
