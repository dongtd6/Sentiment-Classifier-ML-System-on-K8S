# jobs/transforms/batch_prediction.py
import math
import os

import pyspark.sql.functions as F
import requests
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # /job/jobs
CONFIG_PATH = os.path.join(BASE_DIR, "configs", "config.yml")
with open(CONFIG_PATH) as f:
    cfg = yaml.safe_load(f)


def batch_predict(spark, dataframe, logger):
    """
    dataframe: Spark DataFrame có ít nhất cột 'review' (string)
    return: DataFrame gồm 2 cột [review, sentiment]
    """
    PREDICT_URL = cfg["predict"]["api_url"]
    BATCH_SIZE = cfg["predict"]["batch_size"]

    # Lấy list các review cần predict
    rows = (
        dataframe.select("review")
        .where(F.col("review").isNotNull() & (F.length("review") > 0))
        .rdd.map(lambda r: r[0])
        .collect()
    )

    pending_count = len(rows)
    logger.info(f"🕒 Số dòng cần predict: {pending_count}")
    if pending_count == 0:
        logger.info("✅ Không còn dòng nào cần dự đoán. Kết thúc.")
        return dataframe.withColumn("sentiment", F.lit(None).cast("string"))

    total_batches = math.ceil(pending_count / BATCH_SIZE)

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    all_updates = []  # để collect kết quả toàn bộ

    for idx, chunk in enumerate(chunks(rows, BATCH_SIZE), start=1):
        unique_comments = list(dict.fromkeys(chunk))
        logger.info(f"📮 Lô {idx}/{total_batches}: gửi {len(unique_comments)} câu ...")

        try:
            resp = requests.post(
                PREDICT_URL, json={"comments": unique_comments}, timeout=60
            )
            if resp.status_code != 200:
                logger.info(f"❌ API trả mã {resp.status_code}: {resp.text[:200]}")
                continue
            body = resp.json()
            preds = body.get("predictions", [])
        except Exception as e:
            logger.info(f"❌ Lỗi khi gọi API: {e}")
            continue

        if not isinstance(preds, list) or len(preds) == 0:
            logger.info("⚠️ API không trả về 'predictions' hợp lệ.")
            continue

        # Append mapping vào all_updates
        for item in preds:
            cmt = item.get("original_comment")
            sent = item.get("predicted_sentiment")
            if cmt is not None and sent is not None:
                all_updates.append((cmt, sent))

    # Tạo DataFrame kết quả
    if all_updates:
        df_updates = spark.createDataFrame(all_updates, ["review", "sentiment"])
        logger.info(f"📝 Tổng cộng {df_updates.count()} dòng đã được predict")
    else:
        # fallback nếu không có kết quả
        df_updates = spark.createDataFrame([], schema="review string, sentiment string")

    return df_updates
