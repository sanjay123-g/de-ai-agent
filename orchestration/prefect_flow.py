from prefect import flow, task
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from agents.supervisor import run_pipeline, SOURCE_REGISTRY

@task(retries=3, retry_delay_seconds=30)
def run_source(source_cfg: dict) -> dict:
    result = run_pipeline(source_overrides=[source_cfg])
    return result[0]

@flow(name="de-ai-agent-pipeline")
def run_all_sources():
    results = [run_source(cfg) for cfg in SOURCE_REGISTRY]
    return results

if __name__ == "__main__":
    run_all_sources()
