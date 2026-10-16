from observer_finetune.sources.jsonl import JsonlTraceSource
from observer_finetune.split import split_heldout, split_traces


def test_split_keeps_each_question_on_one_side(traces_file):
    traces = JsonlTraceSource(traces_file).load()
    train, heldout, method = split_traces(traces, 0.2, seed=7)
    calib, test = split_heldout(heldout, 0.5, seed=7)
    keys = [{t.question_key for t in part} for part in (train, calib, test)]
    assert not keys[0] & keys[1] and not keys[0] & keys[2] and not keys[1] & keys[2]
    assert len(train) + len(calib) + len(test) == len(traces)
    assert method.startswith("group_by_question")
    again = split_traces(traces, 0.2, seed=7)[0]
    assert [t.trace_id for t in again] == [t.trace_id for t in train]


def test_split_field_wins(traces_file):
    traces = JsonlTraceSource(traces_file).load()
    for t in traces:
        t.split = "heldout" if int(t.qid[1:]) < 180 else "train"
    train, heldout, method = split_traces(traces, 0.2, seed=7)
    assert method == "dataset_split_field"
    assert all(int(t.qid[1:]) < 180 for t in heldout)
