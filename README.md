## GoLevelDB Parser
This project is a Go port of the excellent Python-based [dfindexeddb tool](https://github.com/google/dfindexeddb) created by Google LLC.
The goal of this port is to provide a single, portable, compiled binary for forensic analysis of LevelDB files, without requiring a Python environment. All credit for the original logic, file format research, and structure goes to the creators of the original tool.

Currently my strategy for collecting forensic artifacts is trying to shift-left and have a descentralized approach where we collect from an endpoint just what we need. 

I thought of having this Go version so we can run and filter the results in the same target endpoint, so the results that we bring for post investigation are more meaningful. 

Important: this go version was mainly generated with generative AI.

In any case, please let me know if you have any comments, or suggestions. 

# Usage
./leveldb-parser-go db /path/to/leveldb -o output.json

# Acknowledgements
This tool would not be possible without the foundational work done by the developers of dfindexeddb. Their Python implementation was used as the primary reference for all file parsing logic.
