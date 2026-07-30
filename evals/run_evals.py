"""
evals/run_evals.py

Runs the golden query set through query_agent.answer_question() and
reports: SQL generated, whether it executed successfully, whether it
referenced the expected table, and latency per query.
"""
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.query_agent import answer_question
from evals.golden_queries import GOLDEN_QUERIES


def run():
    results = []
    for case in GOLDEN_QUERIES:
        start = time.time()
        result = answer_question(case["question"])
        elapsed = time.time() - start

        success = result.get("error") is None
        table_hit = (
            result.get("sql")
            and case["expected_table"].lower() in result["sql"].lower()
        )

        results.append({
            "question": case["question"],
            "success": success,
            "correct_table": table_hit,
            "latency_sec": round(elapsed, 2),
            "sql": result.get("sql"),
            "error": result.get("error"),
        })

    total = len(results)
    successes = sum(1 for r in results if r["success"])
    correct_table = sum(1 for r in results if r["correct_table"])
    avg_latency = sum(r["latency_sec"] for r in results) / total

    print(f"\n=== EVAL SUMMARY ===")
    print(f"Execution success rate: {successes}/{total} ({100*successes/total:.0f}%)")
    print(f"Correct table selection: {correct_table}/{total} ({100*correct_table/total:.0f}%)")
    print(f"Average latency: {avg_latency:.2f}s\n")

    for r in results:
        status = "PASS" if r["success"] else "FAIL"
        print(f"[{status}] {r['question']}")
        if not r["success"]:
            print(f"       error: {r['error']}")

    return results


if __name__ == "__main__":
    run()
