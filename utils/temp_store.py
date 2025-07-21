import json, os

FAILED_FILE = "failed_jobs.json"

def save_failed(job):
    with open(FAILED_FILE, 'a') as f:
        f.write(json.dumps(job) + '\n')

def load_failed():
    if not os.path.exists(FAILED_FILE):
        return []
    with open(FAILED_FILE) as f:
        return [json.loads(line.strip()) for line in f]