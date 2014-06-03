#!/bin/bash

IRQ_AFFINITY_FOR_CORE[0]=1
IRQ_AFFINITY_FOR_CORE[1]=2
IRQ_AFFINITY_FOR_CORE[2]=4
IRQ_AFFINITY_FOR_CORE[3]=8
IRQ_AFFINITY_FOR_CORE[4]=10
IRQ_AFFINITY_FOR_CORE[5]=20
IRQ_AFFINITY_FOR_CORE[6]=40
IRQ_AFFINITY_FOR_CORE[7]=80
IRQ_AFFINITY_FOR_CORE[8]=100
IRQ_AFFINITY_FOR_CORE[9]=200
IRQ_AFFINITY_FOR_CORE[10]=400
IRQ_AFFINITY_FOR_CORE[11]=800

function check_app_installed {
  if [ -z "$1" ]; then
    echo "Usage: $0 appname"
    exit 1
  fi
  RES=$(which "$1" 2>&1);
  MISS=$?
  if [ $MISS -eq 1 ]; then return 0;
  else                     return 1; fi
}
function check_required_apps {
  check_app_installed numactl
  OK=$?
  if [ $OK -ne 1 ]; then
    echo "Required application not found: (numactl)"
    exit 1;
  fi
}

function check_is_user_root {
  RES=$(whoami)
  if [ "$RES" != "root" ]; then
    echo "ERROR: $0 must be run as user: root"
    exit 1;
  fi
}

function find_eths {
  RES=$(/sbin/ip link show | grep "state UP")
  NETH=$(echo "${RES}" | wc -l)
  I=0; for eth in $(echo "$RES" | cut -f 2 -d : ); do
    ETH[$I]="$eth"; I=$[${I}+1];
  done
}
# NOTE: if the last word in /proc/interrupt does not have a "-"
#       then it is not an active nic-queue, rather a placeholder for the nic
function validate_eth_q {
  echo $1 |rev | cut -f 1 -d \ | rev | grep \- |wc -l
}
function count_eth_queues {
  find_eths
  NUM_TOT_ETH_QUEUES=0
  I=0; while [ $I -lt $NETH ]; do
    eth="${ETH[$I]}";
    INTERRUPTS=$(grep "$eth" /proc/interrupts)
    GOOD_INTERRUPTS=$(echo "${INTERRUPTS}" |while read intr; do
                       GOOD=$(validate_eth_q "$intr")
                       if [ "$GOOD" == "1" ]; then echo "$intr"; fi
                     done)
    NUM_QUEUES_PER_ETH[$I]=$(echo "${GOOD_INTERRUPTS}" | wc -l)
    NUM_TOT_ETH_QUEUES=$[${NUM_QUEUES_PER_ETH[$I]}+${NUM_TOT_ETH_QUEUES}];
    IRQS_PER_ETH[$I]=$(echo "${GOOD_INTERRUPTS}" | cut -f 1 -d :)
    #echo eth: $eth NUMQ: ${NUM_QUEUES_PER_ETH[$I]} IRQS: ${IRQS_PER_ETH[$I]}
    I=$[${I}+1];
  done
}

function get_num_cpu_sockets {
  NCS=$(numactl --hardware |grep cpus: | wc -l)
}

function get_cpu_socket_cores {
  get_num_cpu_sockets
  NUM_TOT_CPU_CORES=0
  I=0; while [ $I -lt $NCS ]; do
    SOCKET_CORES[$I]=$(numactl --hardware |grep "node $I cpus:" | cut -f 2 -d :)
    NUM_CORE_PER_SOCKET[$I]=$(echo ${SOCKET_CORES[$I]} | wc -w)
    NUM_TOT_CPU_CORES=$[${NUM_CORE_PER_SOCKET[$I]}+${NUM_TOT_CPU_CORES}];
    I=$[${I}+1];
  done
}
