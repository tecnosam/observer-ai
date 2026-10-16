"""The questions Kev is trained on and asked, word for word what the notebooks and the engine use."""

QUESTIONS = {
    "answer_wrong": {
        "type": "noul",
        "instructions": "Is the final answer wrong or only partially correct for the user's question?",
    },
    "keep_or_drop": {
        "type": "choice",
        "instructions": "Should this agent trace be kept for human review?",
        "criteria": {
            "keep": "The final answer is wrong, only partly correct or unsupported by the evidence, or the agent misused or skipped its tools.",
            "drop": "The final answer is correct, complete and supported by the evidence, and the agent used its tools appropriately.",
        },
    },
}

# How to read each signal out of an answer; higher means more worth a human look.
SIGNAL_READ = {
    "answer_wrong": lambda answer: answer["noul"],
    "keep_or_drop": lambda answer: answer["probabilities"]["keep"],
}


def expected_answers(is_wrong: bool) -> dict:
    return {"answer_wrong": is_wrong, "keep_or_drop": "keep" if is_wrong else "drop"}
