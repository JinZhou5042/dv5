# ECF Calculator for High Energy Physics Analysis

Framework for calculating Energy Correlation Functions (ECFs) and other jet substructure variables using Dask and TaskVine for distributed computation in High Energy Physics (HEP).

## Installation

### 1. Create Conda Environment

```bash
unset PYTHONPATH
# Create conda environment 'dv5-env' with Python 3.10
conda create -y -n dv5-env -c conda-forge --strict-channel-priority python=3.10
conda activate dv5-env
# Install build requirements for cctools
conda install -y gcc_linux-64 gxx_linux-64 gdb m4 perl swig make zlib libopenssl-static openssl conda-pack packaging cloudpickle flake8 clang-format threadpoolctl
```

### 2. Build and Install cctools (with TaskVine)

Build and install the required version of cctools:

```bash
# Clone the repository
git clone https://github.com/cooperative-computing-lab/cctools.git
cd cctools
# Configure cctools for the active conda environment
./configure --with-base-dir $CONDA_PREFIX --prefix $CONDA_PREFIX
# Build and install
make -j8 && make install
# Verify installation
vine_worker --version
# Return to the main project directory
cd ..
```

### 3. Install Python Packages

```bash
pip install dask==2024.7.1 dask-awkward==2024.7.0 awkward==2.6.6 coffea==2024.4.0 fastjet==3.4.2.1
```
*These specific versions are recommended for compatibility.*

## Setup

### 1. Clone This Project

Clone this repository to your shared filesystem:
```bash
git clone https://github.com/JinZhou5042/dv5.git
```

### 2. Package Conda Environment for TaskVine Workers

Create a poncho package of the conda environment for TaskVine workers. Run this from a directory accessible by both the manager and workers (shared filesystem).

```bash
# Ensure you are on a shared filesystem
# Package the 'dv5' conda environment
cd dv5
poncho_package_create $CONDA_PREFIX dv5.tar.gz
```
*This might take some time.*

### 3. Configure Resources (factory.json)

The `factory.json` file defines resource requirements (cores, memory, disk) for TaskVine workers.

-   Review and adjust `factory.json` under this directory.
-   If you modify `cores` in `factory.json`, update `lib_resources` in `ecf_calculator.py` accordingly: `lib_resources={'cores': N}` where `N` matches `factory.json`.

### 4. Start TaskVine Workers

In a new terminal with access to the shared filesystem (where the poncho package is), start the TaskVine factory:

```bash
bash run_factory.sh
```
Keep this factory process running during analysis.

## Dataset Preparation

### 1. Directory Structure

Input ROOT files must be organized into subdirectories. The script expects this structure within a `samples/` directory by default:

```
samples/
├── dataset1/      # e.g., hgg
│   ├── file1.root
│   └── file2.root
├── dataset2/      # e.g., hgg_1
│   └── file3.root
└── ...
```
-   **Default Path:** The script defaults to an example path `/groups/dthain/users/jzhou24/dv5-input-samples`. You will likely need to modify the `samples_path` variable inside `ecf_calculator.py` if your data resides elsewhere.

### 2. Dataset Management

-   **Preprocessing Time:** Preprocessing (`--preprocess`) scans all files and can be slow with many datasets.
-   **Selective Preprocessing:** Temporarily move unused dataset directories out of the main data directory before running `--preprocess` to speed it up.
-   **Adding New Datasets:**
    1.  Create a new subdirectory in your main data directory (e.g., `samples/new_dataset`).
    2.  Place ROOT files inside it.
    3.  Re-run `--preprocess`.

## Usage

Run the main script `ecf_calculator.py`:

```bash
python ecf_calculator.py [options]
```

### Core Workflow Steps

1.  **Preprocessing (Required First & After Dataset Changes):**
    Scans the data directory and generates `samples_ready.json`.
    ```bash
    python ecf_calculator.py --preprocess
    ```

2.  **Show Available Samples (Optional):**
    Lists datasets found in `samples_ready.json`.
    ```bash
    python ecf_calculator.py --show-samples
    ```

3.  **Analysis Run (Task Generation & Execution):**
    Generates/loads the Dask task graph and executes it using TaskVine workers.
    ```bash
    # Process default 'hgg' dataset, ECF n<=3
    python ecf_calculator.py

    # Process a specific dataset
    python ecf_calculator.py --sub-dataset hgg_1

    # Process all datasets
    python ecf_calculator.py --all

    # Process all datasets, ECF n<=5
    python ecf_calculator.py --ecf-upper-bound 5 --all
    ```

### DaskVine Configuration Arguments

Fine-tune TaskVine interaction during task execution.

-   `--manager-name <name>`: Connect to a specific TaskVine manager (default: `dv5-manager`).
-   `--run-info-path <path>`: Directory for TaskVine logs/reports. The script may default to a pre-configured shared path (e.g., on AFS) if available and writable in certain environments, otherwise it defaults to `./vine-run-info`. If the specified or default path is inaccessible, it falls back to `./vine-run-info`.
-   `--template <template_name>`: Create a subdirectory `<template_name>` within the effective `run_info_path` for logs/reports.
-   `--enforce-template`: Overwrite the `--template` directory if it exists.
-   `--wait-for-workers <seconds>`: Wait time for workers before starting.
-   `--max-workers <count>`: Limit max workers (default: unlimited).
-   `--temp-replica-count <count>`: Number of replicas for intermediate files (default: 1).
-   `--enforce-worker-eviction-interval <seconds>`: **Testing only.** Force worker eviction periodically.
-   `--load-balancing`: Enable dynamic load balancing.
-   `--prune-depth <depth>`: Dask graph pruning depth (default: 0).

### Examples

1.  **Minimal Workflow:**
    ```bash
    # 1. Preprocess
    python ecf_calculator.py --preprocess
    # 2. Run analysis
    python ecf_calculator.py
    ```

2.  **Analyze Specific Dataset, Higher ECF Bound:**
    ```bash
    python ecf_calculator.py --sub-dataset hgg_2 --ecf-upper-bound 6
    ```

## Output

Analysis results (parquet files) are saved in `output/`, organized by dataset:

```
output/
├── dataset1/      # e.g., output/hgg/
│   ├── part-0.parquet
│   └── ...
├── dataset2/      # e.g., output/hgg_1/
│   └── ...
└── ...
```

## Important Notes

-   **Preprocessing:** Run `--preprocess` if `samples_ready.json` is missing or data directory changes. Preprocessing only creates the JSON.
-   **TaskVine Setup:** Requires a running TaskVine manager and connected workers.
-   **Paths:** Adapt paths for your environment: input data (`samples_path` in `ecf_calculator.py`), TaskVine run info (`--run-info-path`, defaults may vary by environment), poncho package location (shared filesystem).
-   **Output/Log Directories:** `output/` and TaskVine run info dir (`--run-info-path` effective location) are created automatically.
-   **`--show-samples`:** Use to confirm dataset names before using `--sub-dataset`.
-   **`--sporadic-failure` Argument:** Not part of `ecf_calculator.py`; likely for external TaskVine worker testing.
-   **Resource Matching:** Keep `factory.json` (`cores`) and `ecf_calculator.py` (`lib_resources`) synchronized. 