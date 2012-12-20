# Qubota (a queue, a tractor, a dolphin)

An aws based worker queue system

![cow dolphin harmony gets work done](https://pbs.twimg.com/media/A9SjWg2CIAA0aCM.jpg:large)

## Quickstart

 0. Prereqs (for your platform) for these instructions:
    - zeromq (will go away soon)
    - python-dev
    - git
   

 1. Install virtualenvwrapper for your platform (osx or linux supported)
  
 2. Create a virtualenv and populate
 
    ```
    $ mkvirtualenv --no-site-packages qbota
    (qbota)>$ pip install -r https://raw.github.com/whitmo/qubota/master/req.txt
    ```
 3. Create and activate a postactivate hook with your AWS credential vars.
 
     ```
     (qbota)>$ vim $VIRTUAL_ENV/bin/postactivate
     ```

    Take this template and sub with all the {parts} filled in with
    what is pertinent to your situation.
 
    ```
    #!/bin/bash
    export AWS_ACCESS_KEY_ID={access_key_id}
    export AWS_SECRET_ACCESS_KEY={secret_access_key}
    export AWS_AMI={ami}
    export AWS_SIZE={size}
    export AWS_REGION={region}
    export AWS_ZONE={zone}
    export AWS_KEYPAIR={keypair}
    export AWS_SECURITY_GROUPS={security_groups}
    ```
    
    Ex:
    
    ```
    export AWS_ACCESS_KEY_ID=adfaksfasdfgooblygook
    export AWS_SECRET_ACCESS_KEY=spicklemireaiaodfjaiefjadgdvadad
    export AWS_AMI=ami-73fd7e1a
    export AWS_SIZE=t1.micro
    export AWS_REGION=us-east-1
    export AWS_ZONE=us-east-1d
    export AWS_KEYPAIR=smwhit_key
    export AWS_SECURITY_GROUPS=quick-start-1
    export SSH_KEYFILE=~/.ssh/smwhit_key.pem    
    ```
    
    Now source this file:
 
    ```
    (qbota)>$ workon qbota
    ```
  
 4. NOW! Launch your nodes:
 
    ```
    (qbota)>$ qb up
    Bringing up workers: 
    qubota:0 @ ec2-174-129-83-64.compute-1.amazonaws.com    
    ```

 5. Queue work:
    Queue 25 jobs.

    ```
    (qbota)>$ qb nq -n 25 qubota.tests.simple_job
    qubota:439cee88-e9fc-498e-b50c-ad82fbaccf7c
    qubota:c4d00eee-9b8e-4855-ac5c-c77542347170
    qubota:57f33f4b-6de6-49b1-92e8-d18cd62e77ab
    qubota:a3778f1a-c2be-4278-8949-a6d9956c504a
    ```
