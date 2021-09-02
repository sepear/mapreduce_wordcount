# MapReduce Wordcount

It is a client-server system, where the server will be the driver and the clients will be the workers.

When the server boots, it generates a load balance based on the number of tasks, using the number of lines as criteria.
Example of the content of a map task:
{
file 1: (line 23, line 523)
file 2: (line 0, line 200)
}

Once the server is active, we can start the various clients. When starting a worker, an initialization will be carried out first, in which the client will tell the server that it is available and the server will provide the N and M parameters of the task to be solved.

After this, the client will be in a loop requesting tasks (either map or reduce) and solving them until the driver tells it that it has finished. Every time a task finishes, it will notify the driver, so that it updates its status to finished.

As it is necessary that all map tasks have been completed in order to assign reduce tasks, map tasks will always be assigned first. If there are no tasks available but the system has not finished, the driver will tell the worker to wait. When all the tasks have finished, the server will send the signal to the clients to stop, and it will shut down as well.
