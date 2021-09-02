from __future__ import print_function

import logging
import sys
import os

import grpc
import grpc.experimental
from collections import defaultdict

def mapper(task_map, task_id, M):
  '''
  Given the parameters, perform the map task (creating intermediate files)
  We initialize each bucket as an empty string, and then add lines as appropriate.
  Finally, the content is saved in files.
  '''

  print(f"mapper task {task_id} started")
  buckets = ["" for _ in range(M)]
  for key in task_map:
      with open("inputs/"+key) as file:
        lines=file.readlines()

        for line in lines[task_map[key].start:task_map[key].end]:
          words=line.split()
          for word in words:

            for char in '*()-.,;:?!\"':#we clean the word
              word=word.replace(char,' ')

            buckets[ord(word[0])%M]+=f"{word} 1\n"
  for bucket_index in range(M):
    with open(f"files/intermediate/mr-{task_id}-{bucket_index}","w+")as f:
      f.write(buckets[bucket_index])
      
def reducer(task_id):
  '''
  Given a reduce task id, performs the operation and creates the respective out file
  '''
  
  print(f"reducer task {task_id} started")
  accumulator=defaultdict(int)
  directory="files/intermediate"
  #filenames: names of valid buckets for the task
  filenames=[filename  for filename in os.listdir(directory) if filename[-1]==str(task_id)]
  for filename in filenames:
    with open(directory+"/"+filename,"r") as f:
      for line in f:
        accumulator[line.split()[0]]+=1#we add the repetitions in an accumulator

  #once accumulator is done, we write the data
  content=""
  for key,value in accumulator.items():
    content+=f"{key} {value}\n"
  with open("files/out/out-"+str(task_id),"w+") as f:
    f.write(content)



def client():
  '''
  Client function. First it asks the server for the parameters N and M 
  (although only M will be used, N is also asked for consistency).

  As long as the server does not respond that the task has finished,
  it will be in a loop requesting tasks and returning the signal that they have been solved.
   It will also wait if the server so indicates.

  '''
  done=False
  protos, services = grpc.protos_and_services("bidirectional.proto")
  logging.basicConfig()

  response = services.Driver.InitWorker(protos.Ready(state="0"),
                                     'localhost:50051',
                                     insecure=True)
  M=response.M
  N=response.N
  while not done:
    #if response.map has content then its reduce, otherwise it's map task
    response = services.Driver.AssignWork(protos.Ready(state="default_state"),
                                     'localhost:50051',
                                     insecure=True)
    if response.id==-1:#means it's all over
      done=True
    elif response.id==-2:#means it's time to wait
      pass#as tasks are small, adding sleep time doesn't make sense
      
    else:
      if bool(response.map):#if map task
        mapper(response.map,response.id,M)
        response = services.Driver.DoneTask(protos.DoneInfo(id=response.id,task_type="map"),
                                      'localhost:50051',
                                      insecure=True)
      else:#reduce task
        reducer(response.id)
        response = services.Driver.DoneTask(protos.DoneInfo(id=response.id,task_type="reduce "),
                                      'localhost:50051',
                                      insecure=True)
  

if __name__ == '__main__':
  client()
  print("Client done")