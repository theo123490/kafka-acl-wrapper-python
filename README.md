# Kafka acl wrapper
A simple python wrapper to validate kafka acl

---
## How to use
1. Update your environment variables
1. populate your certificates directory in `./certs/`
1. export your environment variables 
```bash
export $(cat .env | xargs)
```
1. make sure you have `confluent_kafka` module installed, for conda 
```
conda install conda-forge::python-confluent-kafka
```
1. run `acl/main.py`
```bash
python3 kafka-python/acl/main.py
```
