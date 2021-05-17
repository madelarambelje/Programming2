import multiprocessing as mp
from multiprocessing.managers import BaseManager, SyncManager
import os, sys, time, queue
from Bio import Entrez
import argparse


Entrez.email = "m.a.delarambelje@gmail.com"
Entrez.api_key = "922bc25a747c178a595054ac75fa5b0f8508"

parser = argparse.ArgumentParser(description='assignment2')
parser.add_argument('-n', '--numChild', type=int, help='number of children', required=True)
parser.add_argument('-p', '--portnumber',type=int ,help='portnumber', required=True)
parser.add_argument('-H', '--hosts', help='hosts', required=True)
parser.add_argument('-a', '--numArticles', type=int, help='number of articles', required=True)
group = parser.add_mutually_exclusive_group()
group.add_argument('-m', '--master', action="store_true", help='master')
group.add_argument('-w', '--worker',action="store_true", help='worker')
args, pmid = parser.parse_known_args()

print(pmid)


def make_server_manager(ip, port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(BaseManager):
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=(ip, port), authkey=authkey)
    manager.start()
    print('Server started at port %s' % port)
    return manager


def runserver(fn, data, IP, PORTNUM, AUTHKEY):
    # Start a shared manager server and access its queues
    manager = make_server_manager(IP, PORTNUM, AUTHKEY)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not data:
        print("Gimme something to do here!")
        return

    print("Sending data!")
    for d in data:
        shared_job_q.put({'fn': fn, 'arg': d})

    time.sleep(2)

    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            #print("Got result!", result)
            if len(results) == len(data):
                print("Got all results!")

                break
        except queue.Empty:
            time.sleep(1)
            continue

    for job in results:
    	article_id = job['job']["arg"]
    	print(article_id)
    	article = (job['result'])
    	with open(f'{article_id}.xml','wb') as f:
    	    f.write(article)
    # Tell the client process no more data will be forthcoming
    print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaand we're done for the server!")
    manager.shutdown()

def make_client_manager(ip ,port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    print('Client connected to %s:%s' % (ip, port))
    return manager


def runclient(num_processes, IP, PORTNUM, AUTHKEY):
    manager = make_client_manager(IP, PORTNUM, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)


def run_workers(job_q, result_q, num_processes):
    processes = []
    for p in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temP)
        temP.start()
    print("Started %s workers!" % len(processes))
    for temP in processes:
        temP.join()


def peon(job_q, result_q):
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print(my_name)
                return
            else:
                try:
                    result = job['fn'](job['arg'])
                    print("Peon %s Workwork on %s!" % (my_name, job['arg']))
                    result_q.put({'job': job, 'result': result})
                except NameError:
                    print("Can't find yer fun Bob!")
                    result_q.put({'job': job, 'result': ERROR})

        except queue.Empty:
            print("sleepytime for", my_name)
            time.sleep(1)
    #print(result.items())


def get_reference(pmid,num):
    '''Extracting first 10 references from user given PMID'''
    # Fetching article with ID
    handle = Entrez.efetch(db="pubmed",id=pmid,retmode="xml")
    record = Entrez.read(handle)
    # Extracting all references
    refList = (record["PubmedArticle"][0].get("PubmedData").get("ReferenceList")[0]["Reference"])
    # Add to empty list all refIDs
    RefIdList = []
    for article in refList:
        refId = article["ArticleIdList"][0].strip()
        RefIdList.append(refId)
    return RefIdList[:num]

def download_save(RefID):
    '''Download reference ID yielded by get_reference'''
    handle = Entrez.efetch(db="pubmed", id=RefID, retmode='xml')
    return handle.read()

    # with open(f'output/{RefID}.xml','wb') as f:
    #     f.write(handle.read())



if __name__ == "__main__":

    POISONPILL = "MEMENTOMORI"
    ERROR = "DOH"
    IP = args.hosts
    PORTNUM = args.portnumber
    AUTHKEY = b'whathasitgotinitspocketsesss?'
    data = ["Always", "look", "on", "the", "bright", "side", "of", "life!"]



    if args.master:
        refID = get_reference(pmid,args.numArticles)
        server = mp.Process(target=runserver, args=(download_save, refID, IP, PORTNUM, AUTHKEY))
        server.start()
        time.sleep(1)
        server.join()
    elif args.worker:
        client = mp.Process(target=runclient, args=(args.numChild,IP,PORTNUM,AUTHKEY))
        client.start()
        client.join()






