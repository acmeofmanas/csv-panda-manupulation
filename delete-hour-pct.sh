#!/usr/bin/env bash

Basepath=$1
Hours=$2
Percentage=$3
Environment=$4

##Hard coded as it is not viable for user to provide workspaces list 
Workspaces=(shared cbgba cbgba-regional hkcbgba)

workspace_logs_base_dir="${Basepath}/${Environment}"
LOG_DIR="${workspace_logs_base_dir}"

retention_by_time() {
  minutes=$((Hours*60))
  echo "Delete workspace log files older than ${Hours} hours"
  find "${workspace_logs_base_dir}" -type f -name "*log.gz" -mmin "+${minutes}" -delete
  ret_code=$?
  if [ $ret_code -eq 0 ]
  then
    echo "Logs older than ${Hours} hours deleted"
  else
    echo "Failed to delete logs older than ${Hours} hours"
  fi
}

check_disk_usage() {
  # Get the total disk size and used space of the logs filesystem
  read TOTAL USED FREE <<< $(df -P "${LOG_DIR}" | tail -n 1 | awk '{print $2,$3,$4}')
  # Calculate the available space percentage
  UsagePercentage=$((($USED*100)/$TOTAL))
  if [ $UsagePercentage -gt $Percentage ]; then
    echo "Disk usage is more than ${Percentage}%, requesting user-session logs deletion from ${LOG_DIR}"
    delete_user_session_logs
  fi
}

delete_user_session_logs() {
  for workspace in ${Workspaces[@]}
  do 
    find "${workspace_logs_base_dir}/${workspace}" -type d -name "${workspace}-user-*" -exec rm -rf {} +
    ret_code=$?
    if [ $ret_code -eq 0 ]; then
      echo "User session logs for ${workspace} deleted"
    else
      echo "Failed to delete user session logs for ${workspace}"
    fi
  done
  check_disk_usage
}

check_disk_usage
retention_by_time
