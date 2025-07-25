import os
import pickle
import sys

import joblib
import pytest

# Ensure the path to the API directory is correct
# Uncomment the following line if you need to add the API directory to the path
# This is useful if your tests are in a different directory than your API code.
# If your API code is in a directory named 'api' at the same level as this test file,
# you can uncomment the line below to include it in the Python path.
# Otherwise, you can remove this line if the API code is already in the Python path.
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'api')))

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_BASE_DIR = os.path.join(CURRENT_TEST_DIR, "..", "model")
SENTIMENT_MODEL_PATH = os.path.join(
    MODEL_BASE_DIR, "model.pkl"
)  # or sentiment_model.pkl
TFIDF_VECTORIZER_PATH = os.path.join(
    MODEL_BASE_DIR, "vectorizer.pkl"
)  # or tfidf_vectorizer.pkl


@pytest.fixture(scope="session")
def loaded_models():
    sentiment_model = None
    tfidf_vectorizer = None
    if not os.path.exists(SENTIMENT_MODEL_PATH):
        pytest.fail(
            f"Model file not found at: {SENTIMENT_MODEL_PATH}. Please ensure your model is trained and saved."
        )
    if not os.path.exists(TFIDF_VECTORIZER_PATH):
        pytest.fail(
            f"Vectorizer file not found at: {TFIDF_VECTORIZER_PATH}. Please ensure your vectorizer is saved."
        )

    try:
        print(
            f"Attempting to load sentiment model from: {os.path.abspath(SENTIMENT_MODEL_PATH)}"
        )
        with open(SENTIMENT_MODEL_PATH, "rb") as f:
            sentiment_model = joblib.load(
                f
            )  # <--- SỬA TỪ pickle.load() SANG joblib.load()
        print("Sentiment model loaded successfully!")

        print(
            f"Attempting to load TF-IDF vectorizer from: {os.path.abspath(TFIDF_VECTORIZER_PATH)}"
        )
        with open(TFIDF_VECTORIZER_PATH, "rb") as f:
            tfidf_vectorizer = joblib.load(
                f
            )  # <--- SỬA TỪ pickle.load() SANG joblib.load()
        print("TF-IDF vectorizer loaded successfully!")

        print("\nModels loaded successfully for testing.")
        return sentiment_model, tfidf_vectorizer
    except Exception as e:
        pytest.fail(f"Failed to load models: {e}")


def test_model_and_vectorizer_loaded(loaded_models):
    """
    Check if the sentiment model and TF-IDF vectorizer are loaded correctly.
    This test ensures that the model and vectorizer are not None and are of the expected types.
    It also checks that the model and vectorizer are loaded from the correct paths.
    """
    sentiment_model, tfidf_vectorizer = loaded_models
    assert sentiment_model is not None, "Sentiment model was not loaded."
    assert tfidf_vectorizer is not None, "TF-IDF vectorizer was not loaded."

    # Check types of the loaded objects
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression

    assert isinstance(sentiment_model, LogisticRegression)
    assert isinstance(tfidf_vectorizer, TfidfVectorizer)


def test_positive_sentiment(loaded_models):
    sentiment_model, tfidf_vectorizer = loaded_models
    text = "Rất tốt, tối thích nó"
    processed_text = [text.lower()]
    text_vectorized = tfidf_vectorizer.transform(processed_text)
    prediction = sentiment_model.predict(text_vectorized)[0]
    assert (
        prediction == "POS"
    ), f"Expected positive sentiment (POS) for '{text}', but got {prediction}"


def test_negative_sentiment(loaded_models):
    sentiment_model, tfidf_vectorizer = loaded_models
    text = "Rất tệ, tôi không thích nó"
    processed_text = [text.lower()]
    text_vectorized = tfidf_vectorizer.transform(processed_text)
    prediction = sentiment_model.predict(text_vectorized)[0]
    assert (
        prediction == "NEG"
    ), f"Expected negative sentiment (NEG) for '{text}', but got {prediction}"


def test_neutral_sentiment(loaded_models):
    sentiment_model, tfidf_vectorizer = loaded_models
    text = "Bình thường, dùng tạm được"
    processed_text = [text.lower()]
    text_vectorized = tfidf_vectorizer.transform(processed_text)
    prediction = sentiment_model.predict(text_vectorized)[0]
    assert (
        prediction == "NEU"
    ), f"Expected neutral sentiment (NEU) for '{text}', but got {prediction}"
