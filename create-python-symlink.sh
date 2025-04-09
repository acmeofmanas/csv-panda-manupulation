#!/bin/bash

# Check if argument is passed
if [ -z "$1" ]; then
    echo "Usage: $0 <python_version>"
    echo "Example: $0 python3.12"
    exit 1
fi

desired_python="$1"
python_path="/usr/bin/$desired_python"

# Check if the desired Python version exists
if [ ! -x "$python_path" ]; then
    echo "Error: $python_path does not exist or is not executable."
    exit 1
fi

# Set up update-alternatives for python
sudo update-alternatives --install /usr/bin/python python "$python_path" 1

# Set it as the default
sudo update-alternatives --set python "$python_path"

# Confirm the result
echo "Python default set to: $(python --version)"
