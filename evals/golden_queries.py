"""
evals/golden_queries.py

Golden benchmark: NL question -> ground-truth SQL pairs.
Tracks whether query_agent produces syntactically valid, guardrail-safe
SQL that executes successfully against the real mart layer.
"""

GOLDEN_QUERIES = [
    {
        "question": "Which team has the highest win percentage?",
        "expected_table": "mart_team_performance",
    },
    {
        "question": "Which team scored the most own goals against them?",
        "expected_table": "mart_goal_analytics",
    },
    {
        "question": "Which team has the youngest average squad age?",
        "expected_table": "mart_squad_profile",
    },
    {
        "question": "List the top 5 teams by total points.",
        "expected_table": "mart_team_performance",
    },
    {
        "question": "Which team has the highest percentage of players at foreign clubs?",
        "expected_table": "mart_squad_profile",
    },
    {
        "question": "Which team scored the most goals in the first half?",
        "expected_table": "mart_goal_analytics",
    },
    {
        "question": "What is the average goal difference across all teams?",
        "expected_table": "mart_team_performance",
    },
    {
        "question": "Which team has the most clean sheets?",
        "expected_table": "mart_team_performance",
    },
    {
        "question": "How many unique goal scorers does each team have?",
        "expected_table": "mart_goal_analytics",
    },
    {
        "question": "Which team has the most goalkeepers in their squad?",
        "expected_table": "mart_squad_profile",
    },
]
