import pytest

PARAGRAPHS = [
    {"title": "Eiffel Tower", "text": "The Eiffel Tower is a wrought-iron lattice tower in Paris, France."},
    {"title": "Paris", "text": "Paris is the capital of France. It lies on the river Seine."},
    {"title": "Mount Everest", "text": "Mount Everest is the highest mountain on Earth, in the Himalayas."},
    {"title": "Photosynthesis", "text": "Photosynthesis is how plants convert light into chemical energy."},
    {"title": "Great Barrier Reef", "text": "The Great Barrier Reef is a coral reef system off Queensland, Australia."},
    {"title": "Python (programming language)", "text": "Python is a high-level programming language created by Guido van Rossum."},
]


@pytest.fixture
def paragraphs():
    return [dict(p) for p in PARAGRAPHS]
