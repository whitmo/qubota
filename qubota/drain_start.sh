#!/bin/bash  
su ec2-user -c "bash -l -c cd /home/ec2-user/app/qubota/src/qubota && git pull --rebase origin master "
start drain
sleep 4s
status drain > /init-report.log
tail /var/log/drain.log >> /init-report.log

