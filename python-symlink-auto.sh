#!/bin/bash

# Find the latest installed Python version in /usr/bin
latest_python=$(ls /usr/bin/python3.* | grep -Eo 'python3\.[0-9]+' | sort -V | tail -n 1)

if [ -z "$latest_python" ]; then
    echo "No Python 3 versions found in /usr/bin."
    exit 1
fi

latest_path="/usr/bin/$latest_python"

# Set up update-alternatives for python
sudo update-alternatives --install /usr/bin/python python $latest_path 1

# Set it as the default
sudo update-alternatives --set python $latest_path

# Confirm the result
echo "Python default set to: $(python --version)"
