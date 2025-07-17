import os
from contextlib import asynccontextmanager
from functools import wraps

import joblib
from fastapi import FastAPI, HTTPException, Request

# from utils.logging import logger
from loguru import logger
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import get_tracer_provider, set_tracer_provider
from schema import (
    BatchPredictRequest,
    BatchPredictResponse,
    HealthCheckResponse,
    PredictRequest,
    PredictResponse,
)

# --- Config OpenTelemetry for Jaeger ---
set_tracer_provider(TracerProvider(resource=Resource.create({SERVICE_NAME: "tsc"})))
tracer = get_tracer_provider().get_tracer("tsc", "1.0.1")
jaeger_exporter = JaegerExporter(
    agent_host_name="localhost",  # Jaeger Agent adress
    agent_port=6831,  # Port of Jaeger Agent
)
span_processor = BatchSpanProcessor(jaeger_exporter)
get_tracer_provider().add_span_processor(span_processor)
# --- Finish config OpenTelemetry ---


# --- Custom Trace Decorator ---
def trace_span(span_name: str):
    """
    Decorator to create a span OpenTelemetry for async function.
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Create a new span with the given name
            with tracer.start_as_current_span(span_name) as span:
                try:
                    result = await func(
                        *args, **kwargs
                    )  # "Inside function!": call the original function
                    return result
                except Exception as e:
                    # log the exception and set attributes on the span
                    span.set_attribute("error", True)
                    span.record_exception(e)
                    raise  # re-raise the exception to propagate it
                finally:
                    pass  # "After execution": Span will be automatically ended when exiting the context

        return wrapper

    return decorator


# --- Model Loading Configuration ---
MODEL_DIR = "./model"
MODEL_PATH = os.path.join(MODEL_DIR, "model.pkl")
VECTORIZER_PATH = os.path.join(MODEL_DIR, "vectorizer.pkl")


# --- FastAPI Lifespan (Load/Unload Models) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager để tải model và vectorizer khi ứng dụng khởi động.
    Sử dụng app.state để lưu trữ model thay vì biến global.
    """

    # ... Application Start Logic ...
    with tracer.start_as_current_span("model-loader") as span:
        logger.info(
            "Application startup: Loading sentiment model and TF-IDF vectorizer..."
        )
        try:
            if not os.path.exists(MODEL_PATH) or not os.path.exists(VECTORIZER_PATH):
                span.set_attribute("error", True)
                span.record_exception(
                    FileNotFoundError(
                        f"Model or vectorizer file not found: {MODEL_PATH}, {VECTORIZER_PATH}"
                    )
                )
                raise FileNotFoundError(
                    f"Error: Model or vectorizer files not found. "
                    f"Check paths: {MODEL_PATH} and {VECTORIZER_PATH}"
                )
            app.state.sentiment_model = joblib.load(MODEL_PATH)
            app.state.tfidf_vectorizer = joblib.load(VECTORIZER_PATH)
            logger.info("Application startup: Models loaded successfully.")
            span.set_attribute("model_loaded_status", "success")
        except Exception as e:
            logger.error(f"Application startup error: Failed to load models - {e}")
            app.state.sentiment_model = None
            app.state.tfidf_vectorizer = None
            span.set_attribute("error", True)
            span.record_exception(e)
            span.set_attribute("load_error_message", str(e))
    yield  # Yield control to the application

    # --- Application Shutdown Logic ---
    logger.info("Application shutdown: Releasing resources...")
    # del model  # Delete reference
    # gc.collect()  # Force garbage collection

    app.state.sentiment_model = None
    app.state.tfidf_vectorizer = None
    get_tracer_provider().force_flush()  # Ensure all spans are flushed before shutdown
    logger.info("Application shutdown: Resources released successfully.")
    span.set_attribute("shutdown_status", "success")
    span.set_attribute("tracer_flush_status", "success")
    span.end()  # Kết thúc span khi ứng dụng dừng
    logger.info("Application shutdown: Span ended successfully.")


# --- Initialize FastAPI App ---
app = FastAPI(
    title="Simple Sentiment Analysis API",
    description="API to classify sentiment (Positive, Negative, Neutral) using a traditional ML model.",
    version="1.0.0",
    lifespan=lifespan,
)


# --- Helper Function to Check Model Status ---
@trace_span("check-models-loaded")
def check_models_loaded():
    # Kiểm tra xem model và vectorizer đã được tải hay chưa
    sentiment_model = app.state.sentiment_model
    tfidf_vectorizer = app.state.tfidf_vectorizer
    logger.info("Checking if models are loaded...")
    if sentiment_model is None or tfidf_vectorizer is None:
        raise HTTPException(
            status_code=503,
            detail="Models not loaded. Server is not ready yet or failed to load models.",
        )


# --- Prediction Endpoint (Single Comment) ---
@trace_span("predict-single-comment-endpoint")
@app.post(
    "/predict/",
    response_model=PredictResponse,
    summary="Predict sentiment for a single comment",
    description="Analyzes the sentiment of a product comment and returns the predicted category (POS, NEG, NEU).",
)
async def predict_single_comment(request: PredictRequest):
    logger.info("Make predictions...")
    # Access the model and vectorizer from app.state
    sentiment_model = app.state.sentiment_model
    tfidf_vectorizer = app.state.tfidf_vectorizer
    # Check if models are loaded
    check_models_loaded()
    comment_vectorized = tfidf_vectorizer.transform([request.comment])
    predicted_label = sentiment_model.predict(comment_vectorized)[0]
    logger.info(f"Prediction complete: {predicted_label}")
    return PredictResponse(
        original_comment=request.comment, predicted_sentiment=predicted_label
    )


# --- Prediction Endpoint (Batch Comments) ---
@trace_span("predict-batch-comments-endpoint")
@app.post(
    "/predict_batch/",
    response_model=BatchPredictResponse,
    summary="Predict sentiment for multiple comments",
    description="Analyzes the sentiment of a list of product comments and returns the predicted categories.",
)
async def predict_batch_comments(request: BatchPredictRequest):
    logger.info(
        f"Received batch prediction request for {len(request.comments)} comments."
    )
    # Access the model and vectorizer from app.state
    sentiment_model = app.state.sentiment_model
    tfidf_vectorizer = app.state.tfidf_vectorizer
    if not request.comments:
        raise HTTPException(
            status_code=400, detail="No comments provided for batch prediction."
        )
    if len(request.comments) > 1000:
        raise HTTPException(
            status_code=400,
            detail="Batch size exceeds the maximum limit of 1000 comments.",
        )
    logger.info("Vectorizing comments for batch prediction...")
    # Check if models are loaded
    check_models_loaded()
    comments_vectorized = tfidf_vectorizer.transform(request.comments)
    predicted_labels_raw = sentiment_model.predict(comments_vectorized)
    results = [
        PredictResponse(original_comment=comment, predicted_sentiment=label)
        for comment, label in zip(request.comments, predicted_labels_raw)
    ]
    logger.info(f"Batch prediction complete: {len(results)} predictions made.")
    # Return the results as a BatchPredictResponse
    return BatchPredictResponse(predictions=results)


# --- Health Check Endpoint ---
@trace_span("health-check-endpoint")
@app.get(
    "/health",
    response_model=HealthCheckResponse,
    summary="Health check",
    description="Checks the health status of the API and model loading.",
)
def health_check():
    logger.info("Performing health check...")
    # Access the model and vectorizer from app.state
    sentiment_model = app.state.sentiment_model
    tfidf_vectorizer = app.state.tfidf_vectorizer
    # Check if models are loaded
    check_models_loaded()
    # Return health status
    is_model_loaded = sentiment_model is not None and tfidf_vectorizer is not None
    status = "healthy" if is_model_loaded else "degraded"
    message = (
        "API is healthy and models are loaded."
        if is_model_loaded
        else "API is running but models failed to load. Predictions may not work."
    )
    logger.info(f"Health check complete: Models loaded status is {is_model_loaded}.")
    return HealthCheckResponse(
        status=status, message=message, model_loaded=is_model_loaded
    )
