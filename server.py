
from concurrent import futures
import logging

import grpc
import os

import subprocess
from collections import defaultdict

protos, services = grpc.protos_and_services("bidirectional.proto")



class Driver(services.DriverServicer):
    '''
    System Server (Driver):

    It is in charge of balancing the load,
 assigning tasks to workers and ending the system
    '''

    def __init__(self,N,M):
        self.N=N
        self.M=M
        self.map_tasks = self.balance_map_tasks()
        self.map_status=[0 for _ in range(self.N)]# zero means not assigned, 1 assigned and 2 completed
        self.reduce_status=[0 for _ in range(self.M)]# zero means not assigned, 1 assigned and 2 completed
        self.client_count=0
        self.map_done=False#to check if we can start with reduce tasks
        self.done=False#to chek if we can end 
        if not os.path.exists("files"):
            os.mkdir("files")
            os.mkdir("files/intermediate")
            os.mkdir("files/out")


    def balance_map_tasks(self):
        '''
        We divide the load of the tasks using the number of lines as a criterion
        (each task will have the same number of lines assigned, assuming that it
        will be equivalent to a balanced load)

        '''
        inputs = os.listdir("inputs")
        in_iterator = iter(inputs)
        n_lines = 0
        memory = dict()
        tasks = defaultdict(dict)
        for filename in inputs:
            file_len = int(subprocess.check_output(f"wc -l inputs/{filename}", shell=True).split()[0])
            memory[filename] = file_len
            n_lines += file_len
            
        section_size = int(n_lines/self.N)
        in_iterator = iter(inputs)
        actual_file=next(in_iterator,None)
        actual_line=0

        for task_index in range(self.N):
            remaining_size=section_size   
            done_task=False#checks if task asignation is done    
            while not done_task:
                if memory[actual_file]>actual_line+remaining_size:#cabe todo el tamaño en este fichero
                    if task_index==self.N-1:#to the end so we take those remaining lines due to oddity
                        tasks[task_index][actual_file]=protos.Pair(start=actual_line,end=memory[actual_file])#start,end
                    else:
                        tasks[task_index][actual_file]=protos.Pair(start=actual_line,end=actual_line+remaining_size)#start,end
                    actual_line+=remaining_size
                    done_task=True
                else:
                    tasks[task_index][actual_file]=protos.Pair(start=actual_line,end=memory[actual_file])#no hace falta sumar, salen las cuentas
                    remaining_size-=(memory[actual_file]-actual_line)
                    actual_file=next(in_iterator,None)
                    actual_line=0    

        return tasks#dictionary with map tasks



    def AssignWork(self, request, context):
        '''
        Assigns tasks to workers. First try assigning map, then reduce. 
        If there are no available tasks, it sends the wait signal,
         and if the system has finished, it sends the shutdown signal.
        '''
        
        if not self.done:
            if not self.map_done:#first we try to assign map tasks
                for i in range(self.N):
                    if self.map_status[i]==0:
                        work=protos.Work(map=self.map_tasks[i],id=i)
                        self.map_status[i]=1#assigned
                        print(f"map task {i} assigned ")
                        return work
                #at this point, worker has to wait
                work=protos.Work(map={},id=-2)#-2 means wait
                return work
            else:

                #then, we try to assign reduce tasks
                for i in range(self.M):
                    if self.reduce_status[i]==0:
                        work=protos.Work(map={},id=i)
                        self.reduce_status[i]=1#assigned
                        print(f"map task {i} assigned ")
                        return work
                work=protos.Work(map={},id=-2)#-2 means wait
                return work

        else:#if done, send message to turn off
            work=protos.Work(map={},id=-1)
            self.client_count-=1#this client will be turned off
            if self.client_count==0:
                server.stop(0.5)
                print("Server off")
            return work
    def InitWorker(self, request, context):
        '''
        We initialize the new worker, sending him the execution parameters
        and taking it into account in the count of active workers

        '''
        self.client_count+=1

        return protos.Metadata(M=self.M,N=self.N)

    def DoneTask(self, request, context):
        '''
        The client communicates to the server the task it has finished and its type

        '''

        if request.task_type=="map":
            self.map_status[request.id]=2#2 means done
            if all(elem==2 for elem in self.map_status):
                self.map_done=True

        else:
            self.reduce_status[request.id]=2#2 means done
            if all(elem==2 for elem in self.reduce_status):
                self.done=True
                


        return protos.Ready(state="ok")



        

def serve():
    global server#global to call it from Driver to shutdown
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    services.add_DriverServicer_to_server(Driver(N=6,M=4), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()