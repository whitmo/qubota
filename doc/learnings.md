================
 Things learned
================

Cloud-init
==========

Leave the smallest shortest possible steps to cloud-init:

 * setting up credentials
 * pulling latests application code and any updating build steps ala
   running pip install -e on application library for a python app.

Speed and failure
-----------------

Delegating more intensive install steps leads to potential points of
failure.  

Note: having fuller build steps is good for automating ami creation
and maintenance (thereby cutting time for dealing with amis). Just not
worth the time eveverytime.

Technique / Debugging
---------------------

Amazon linux amis run a pretty old version of cloud-init.  Many of the
features you might like to use in cloud-config (writing files), do not
work.  some like runcmd will give you fairly surprising results and
should be avoided.

What does work:

 * in cloud config: basic package installation and package system update 
 * shell scripts: The will be run in alphabetic/numeric order based on
   the name of the script.
 * upstart scripts


Upstart
=======

Again, the version on AL lags far behind the head of upstart development.  Newer versions seem to have better support for running commands under a particular user and capture stderr and out.

In the version with AL (0.6), I had to do this to run under a particular user:

  `exec su ec2-user -s '/bin/bash' -c "bash -l -c 'workon qubota && qb -v --debug drain >> $LOG_FILE 2>&1'"`


SimpleDB
========

  * There is a 1024byte limit for attribute values 
  * certain characters can cause strange errors from `boto`:
   ``` 
   <?xml version="1.0"?>
   <Response><Errors><Error><Code>SignatureDoesNotMatch</Code>
   <Message>The request signature we calculated does not match the signature you provided. Check 
   your AWS Secret Access Key and signing method. Consult the service documentation for details.
   </Message></Error>
   </Errors>
   <RequestID>73963b12-e13d-c3f5-f857-4622011b9b01</RequestID>
   </Response>
   <Drone at 0x1100c9910> failed with SDBResponseError
   ```
  
  This error does not actually occur due to anything related to auth
  as far as I can tell, but actually may result from unexpected
  unescaped characters.
   
  * Flat tends to work better for serialization than nest for items.


