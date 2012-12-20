#!/bin/bash  
wget https://raw.github.com/whitmo/qubota/master/req.txt -O /tmp/req.txt
chmod 666 /tmp/req.txt
chown -R ec2-user:ec2-user /home/ec2-user/app/qubota
su ec2-user -c "bash -l -c 'mkvirtualenv --python=python27 /home/ec2-user/app/qubota'"
su ec2-user -c "/bin/bash -l -c 'workon qubota && pip install -r /tmp/req.txt'"
echo DONE > /tmp/mkenv-done
