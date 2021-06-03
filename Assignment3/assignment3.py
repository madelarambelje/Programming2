import argparse
import multiprocessing as mp
import os
import socket
import paramiko
import queue
import sys
import time
import socket
from Bio import Entrez
from multiprocessing import Process, Pool
from multiprocessing.managers import BaseManager, SyncManager

Entrez.email = "m.a.delarambelje@gmail.com"
Entrez.api_key = "922bc25a747c178a595054ac75fa5b0f8508"


class QueueManager(BaseManager):
    pass


class Master:

    def __init__(self, fn, data, ip, port, authkey, hosts, processes):
        self.fn = fn
        self.data = data
        self.ip = ip
        self.port = port
        self.authkey = authkey
        self.hosts = " ".join(hosts)
        self.hosts_con = hosts
        self.processes = processes

    def command(self):

        self.server = Connection(ip=self.ip).con()
        stdin,stdout, stderr = self.server.exec_command(f"python3 /homes/madelarambelje/Programming2/Assignment3/assignment3.py -m -p {self.port} -H {self.hosts} -n {self.processes}",  get_pty=True)
        for line in iter(stdout.readline, ""):
            print(line, end="")

    def make_server_manager(self):

        """ Create a manager for the server, listening on the given port.
            Return a manager object with get_job_q and get_result_q methods.
        """
        job_q = queue.Queue()
        result_q = queue.Queue()
        # This is based on the examples in the official docs of multiprocessing.
        # get_{job|result}_q return synchronized proxies for the actual Queue
        # objects.
        QueueManager.register('get_job_q', callable=lambda: job_q)
        QueueManager.register('get_result_q', callable=lambda: result_q)

        manager = QueueManager(address=(self.ip, self.port), authkey=self.authkey)
        manager.start()
        print('Server started at port %s' % self.port)
        return manager

    def suicide(self):
        self.server.close()
        print("master killed")

    def connect_workers(self):
        print("Starting workers")
        self.fire_me_up = WorkerFactory(hosts=self.hosts_con, port=self.port, processes=self.processes)
        self.workers = self.fire_me_up.connecting_workers()

    def fire_up_workers(self):
        self.info = self.fire_me_up.run_worker_command()
        for line in iter(self.info.readline,""):
            print(line, end="")

    def kill_workers(self):
        for worker in self.workers:
            worker.close()
            print("Closed connection with")

    def runserver(self):

        # Start a shared manager server and access its queues
        manager = self.make_server_manager()
        shared_job_q = manager.get_job_q()
        shared_result_q = manager.get_result_q()

        if not self.data:
            print("Gimme something to do here!")
            return

        print("Sending data!")
        for d in self.data:
            shared_job_q.put({'fn': self.fn, 'arg': d})

        time.sleep(2)

        results = []
        while True:
            try:
                result = shared_result_q.get_nowait()
                results.append(result)
                if len(results) == len(self.data):
                    print("Got all results!")

                    break
            except queue.Empty:
                time.sleep(1)
                continue
       # Tell the client process no more data will be forthcoming
        print("Time to kill some peons!")
        shared_job_q.put(None)
        # Sleep a bit before shutting down the server - to give clients time to
        # realize the job queue is empty and exit in an orderly way.
        time.sleep(5)
        print("Aaaaaand we're done for the server!")
        manager.shutdown()

    def starting_up_server(self):
        server = mp.Process(target=self.runserver)
        server.start()
        time.sleep(1)
        server.join()


class Connection:


    def __init__(self,ip):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ip, timeout=5)
        self.server = client
        print(f"Started Connection at {ip}")

    def con(self):
        return self.server


class WorkerFactory:
    """This function requires the worker ips provided by the
    user and fires up the workers and returns a pool of workers"""

    def __init__(self, hosts, port, processes):
        self.hosts = " ".join(hosts)
        self.hosts_con = hosts
        self.port = port
        self.processes = processes
    def connecting_workers(self):
        workers = []
        for ip in self.hosts_con[1:]:
            print(ip)
            worker = Connection(ip=ip).con()
            workers.append(worker)
        self.workers = workers
        return self.workers

    def run_worker_command(self):
        self.stdout_worker = []
        for worker in self.workers:
            stdin,self.stdout, stderr = worker.exec_command(f"python3 /homes/madelarambelje/Programming2/Assignment3/assignment3.py -w -p {self.port} -H {self.hosts} -n {self.processes}", get_pty=True)
        return self.stdout

class ServerQueueManager(BaseManager):
    pass

class Workers(mp.Process):

    def __init__(self ,ip, port, authkey, num_processes):
        mp.Process.__init__(self)
        self.ip = ip
        self.port = port
        self.authkey = authkey
        self.num_processes = num_processes
        self.manager = self.make_client_manager()
        self.job_q = self.manager.get_job_q()
        self.result_q = self.manager.get_result_q()


    def make_client_manager(self):
        """ Create a manager for a client. This manager connects to a server on the
            given address and exposes the get_job_q and get_result_q methods for
            accessing the shared queues from the server.
            Return a manager object.
        """


        ServerQueueManager.register('get_job_q')
        ServerQueueManager.register('get_result_q')

        manager = ServerQueueManager(address=(self.ip, self.port), authkey=self.authkey)
        manager.connect()

        print('Client connected to %s:%s' % (self.ip, self.port))
        return manager
    def startup_worker(self):
        client = mp.Process(target=self.run_client)
        client.start()
        client.join()

    def run_client(self):
        self.run_workers()

    def run_workers(self):

        processes = []
        for p in range(self.num_processes):
            temP = mp.Process(target=self.peon)
            processes.append(temP)
            temP.start()
        print("Started %s workers!" % len(processes))
        for temP in processes:
            temP.join()

    def peon(self):
        my_name = mp.current_process().name
        while True:
            try:
                job = self.job_q.get_nowait()
                if job == None:
                    self.job_q.put(None)
                    print(my_name)
                    return
                else:
                    try:
                        result = job['fn'](job['arg'])
                        print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                        self.result_q.put({'job': job, 'result': result})
                    except NameError:
                        print("Can't find yer fun Bob!")
                        self.result_q.put({'job': job, 'result': ERROR})

            except queue.Empty:
                print("sleepytime for", my_name)
                time.sleep(1)




data = ["Always", "look", "on", "the", "bright", "side", "of", "life!"]

if __name__ == "__main__":

    # Commandline arguments
    parser = argparse.ArgumentParser(description='assignment2')
    parser.add_argument('-n', '--cores', type=int, help='number of children', required=True)
    parser.add_argument('-p', '--portnumber', type=int, help='portnumber', required=True)
    parser.add_argument('-H', '--hosts',nargs="+" ,help='hosts', required=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-m', '--master', action="store_true", help='master')
    group.add_argument('-w', '--worker', action="store_true", help='worker')
    args = parser.parse_args()
    cores = args.cores
    port = args.portnumber
    master = args.master
    worker = args.worker
    hosts = args.hosts


    def get_reference(pmid, num):
        '''Extracting first 10 references from user given PMID'''
        # Fetching article with ID
        handle = Entrez.efetch(db="pubmed", id=pmid, retmode="xml")
        record = Entrez.read(handle)
        # Extracting all references
        refList = (record["PubmedArticle"][0].get("PubmedData").get("ReferenceList")[0]["Reference"])
        # Add to empty list all refIDs
        RefIdList = []
        for article in refList:
            refId = article["ArticleIdList"][0].strip()
            RefIdList.append(refId)
        return RefIdList[:num]


    hello = get_reference("30881919", 10)
    ERROR = "DOH"

    def download_save(RefID):
        '''Download reference ID yielded by get_reference'''
        handle = Entrez.efetch(db="pubmed", id=RefID, retmode='xml')
        with open(f'/homes/madelarambelje/Programming2/Assignment3/{RefID}.xml', 'w') as f:
            f.write(handle.read())

    if args.master:
        if socket.gethostname() == hosts[0]:
            master = Master(download_save, hello, ip=hosts[0], port=port, authkey=b'lala', hosts=hosts, processes=cores)
            master.starting_up_server()
        else:
            master = Master(download_save, hello,  ip=hosts[0], port=port, authkey=b'lala', hosts=hosts, processes=cores)
            master.connect_workers()
            m = Process(target=master.command)
            m.start()
            master.fire_up_workers()
            m.join()

    elif args.worker:
        time.sleep(2)
        workers = Workers(ip=hosts[0], port=port, authkey=b'lala', num_processes=cores)
        workers.startup_worker()






