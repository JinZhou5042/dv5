## Prerequisites

- Python environment with required packages:
  - dask (2024.7.1)
  - dask-awkward (2024.7.0)
  - awkward (2.6.6)
  - coffea (2024.4.0)
  - fastjet (3.4.2.1)
  - scipy (1.14.0)
  - ndcctools

These versions have been tested and are known to work with this code. Other versions may work but are not guaranteed.

## Dataset Structure

The input data should be organized in the following structure:
```
samples/
├── hgg/           # Each directory represents a sub-dataset
├── hgg_1/         # You can process all sub-datasets or
├── hgg_2/         # choose to process only one specific
├── hgg_3/         # sub-dataset at a time
└── ...
```

Each sub-dataset directory should contain the input ROOT files.

### Dataset Management

- If you have too many sub-datasets, preprocessing might take a long time
- To speed up preprocessing:
  1. You can temporarily remove or move unnecessary sub-datasets from the `samples/` directory
  2. After preprocessing, you can add them back for future runs
- To create new sub-datasets:
  1. Simply copy an existing sub-dataset directory
  2. Rename it to your desired name
  3. The new sub-dataset will be automatically included in the next preprocessing

## Usage

The script can be run with the following command-line arguments:

```bash
python ecf_calculator.py [options]
```

### Required Steps

1. First, you need to preprocess the data to prepare the dataset information:
```bash
python ecf_calculator.py --preprocess
```
This will create a `samples_ready.json` file containing the processed data information and then exit. You need to run the analysis separately.

2. Then, in a separate command, you can run the analysis:
```bash
python ecf_calculator.py
```

### Optional Arguments

- `--ecf-upper-bound`: Upper bound for ECF calculation (n=2 to n=ecf-upper-bound). Default is 3. Valid values are 3, 4, 5, or 6. Larger number will take longer to compute.
- `--all`: Process all sub-datasets. Without this flag, only processes the specified sub-dataset
- `--show-samples`: Display available samples and their file counts
- `--sub-dataset`: Specify which sub-dataset to process. Must be one of the sub-datasets shown by `--show-samples` (default: hgg)

### Examples

1. Basic analysis for testing (process only hgg sub-dataset with basic ECFs):
```bash
python ecf_calculator.py
```
This is the simplest example. You can combine different options based on your needs.

2. First check available sub-datasets:
```bash
python ecf_calculator.py --show-samples
```
This will show you a list of all available sub-datasets and their file counts.

3. Process a specific sub-dataset with extended ECFs:
```bash
python ecf_calculator.py --ecf-upper-bound 6 --sub-dataset hgg_1
```

4. Process all sub-datasets with extended ECFs:
```bash
python ecf_calculator.py --ecf-upper-bound 6 --all
```

5. Complete workflow (preprocess and then run analysis):
```bash
# First run preprocessing
python ecf_calculator.py --preprocess

# Then run the analysis (in a separate command)
python ecf_calculator.py --ecf-upper-bound 6
```

## Output

The results will be saved in the `ecf_calculator_output/{dataset}/` directory, where `{dataset}` is the name of the processed sub-dataset.

## Notes

- Make sure to run `--preprocess` first if `samples_ready.json` does not exist
- The preprocessing step will exit after creating `samples_ready.json`
- You need to run the analysis in a separate command after preprocessing
- The code uses DaskVine for distributed computing
- The output directory will be created automatically if it doesn't exist
- You can process either all sub-datasets or choose to process a specific one at a time
- The basic analysis command (`python ecf_calculator.py`) is just a simple example - you can combine different options based on your needs
- Always use `--show-samples` to see the list of available sub-datasets before specifying `--sub-dataset`
- The value of `--sub-dataset` must match exactly one of the sub-datasets shown by `--show-samples`
- If preprocessing is taking too long, consider temporarily removing unnecessary sub-datasets from the `samples/` directory 