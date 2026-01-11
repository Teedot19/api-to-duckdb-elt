import pandas as pd
from typing import List

from ingestion_pipeline.models.news_article import NewsArticle


def articles_to_dataframe(validated_articles: List[NewsArticle]) -> pd.DataFrame:
    
    results_df = pd.DataFrame(validated_articles)

    schema_map = {
        "uuid": "string",
        "title": "string",
        "description": "string",
        "snippet": "string",
        "url": "string",
        "image_url": "string",
        "published_at": "string",  # Keep as string before to_datetime
        "source": "category",
        "locale": "category",
        "categories": "object",
        "keywords": "string",
    }

    df_parsed = results_df.astype(schema_map)
    df_parsed["published_at"] = pd.to_datetime(df_parsed["published_at"])

    return df_parsed
