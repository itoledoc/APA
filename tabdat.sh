#!/bin/bash
PATH=/users/aod/anaconda/bin:$PATH:/users/aod/Qt/bin:/users/aod/gWTO2:/users/aod/bin
export PATH
export QTDIR=/users/aod/Qt
export QTINC=/users/aod/Qt/include
export QTLIB=/users/aod/Qt/lib
export WTO=/users/aod/gWTO2/
export APA=/users/aod/APA/
export PHASEONE=/users/aod/PhaseI/
export CON_STR="almasu/alma4dba@ALMA_ONLINE.OSF.CL"

PYTHONPATH=/users/aod/gWTO2:/users/aod/APA:/users/aod/anaconda
export PYTHONPATH
export TNS_ADMIN=/opt/DataPacker
export ORACLE_HOME=/opt/oracle/instantclient_11_2
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/opt/oracle/instantclient_11_2/lib

python /users/aod/bin/refresh_apa.py
scp /users/aod/data/summary_table.csv Ignacio1@10.200.113.78:Documents/tabdata/.
scp /users/aod/data/summary_table.csv Ignacio1@10.200.113.78:Downloads/.
scp /users/aod/data/aquaexe.csv Ignacio1@10.200.113.78:Documents/tabdata/.
scp /users/aod/data/dates.csv Ignacio1@10.200.113.78:Documents/tabdata/.
scp /users/aod/data/sim.csv Ignacio1@10.200.113.78:Documents/tabdata/.
scp /users/aod/data/shiftlog.csv Ignacio1@10.200.113.78:Documents/tabdata/.
