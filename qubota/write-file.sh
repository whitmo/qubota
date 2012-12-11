#!/bin/bash -l
mkdir -p "{parent}"
cat >"{filepath}" <<EOF
{content}
EOF 
