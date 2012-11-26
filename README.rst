===========
 AWS Queue
===========

An experiment.

Bootstrapping
=============

AMI: ami-9c78c0f5

Cloud Init: https://help.ubuntu.com/community/CloudInit
Salt: http://docs.saltstack.org/en/latest/topics/tutorials/bootstrap_ec2.html



AWS Async Workflow Investigation
================================

Overview
--------

Use AWS services SQS, SimpleDB, and EC2 to manage async work jobs.


Deliverables
------------

Build three components:

A. client side python library to enqueue and get status of jobs (the
ENQUEUER);

B. queue drainer (the DRAINER);

C. status wsgi (the STATUS PAGE);

And lastly,

D. derive a formula to calculate cost per job.


Components
----------

The ENQUEUER should be a simple python class that is to be linked into
the client app. The class ctor takes a queue name, and a visibility
timeout. There is a method enqueue that takes a dictionary of string
name, value pairs. This method creates a simpledb row with the payload
under a unique job_id (perhaps prefixed with the queuename to get a
namespace), and time enqueud, and a state. Enqueue the job to SQS.
Return the job_id.

The DRAINER is a process that reads from the queue, updates state of
the job in SimpleDB, executes the job, updates the state, then deletes
the message from the queue.

The STATUS page is a wsgi running on a well known port that shows teh
number of jobs in the queue, the number of drainers running on this
box, and the number of boxen draining the queue (instances can have
tags, you can list tags).  Perhaps a "sepuku" button and a "spawn"
button as well?

The drainer and the status page are launched by a pre-build AMI that
takes the queue name as a parameter.

The idea is that we can launch k instances of these queue drainers to
drain a queue, http to any one of them on the port and get a status of
progress.  For this exercise the job itself can be sleep(x) where x is
in the payload or something.  Find prime numbers. Search for ETs.


Cost
----

We should be able to calculate the total average cost per job given
the following inputs:

* size of job payload in bytes
* time to run a single job
* number of concurrent jobs per box

Costs:

* bandwidth in
* bandwidth out
* service calls
* bytes stored
* cpu time

Guesses anyone? I'm going $1 per million jobs.