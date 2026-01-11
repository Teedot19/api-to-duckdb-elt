from dataclasses import dataclass, replace,asdict, field
from typing import Optional, Dict, Any, List, Iterable 



@dataclass(frozen=True)  
class NewsArticle:
    uuid: str
    title: str
    description: str
    snippet: str
    url: str
    image_url: str
    published_at: str  
    source: str
    locale: str
    categories: List[str] = field(default_factory=list)
    keywords: Optional[str] = None



    @classmethod
    def from_dict(cls, data: dict):
        """Creates a single article, filtering out extra API fields."""
        return cls(**{
            k: v for k, v in data.items() 
            if k in cls.__dataclass_fields__
        })

    @classmethod
    def from_list(cls, data_list: List[dict]) -> List['NewsArticle']:
        """Converts a list of dictionaries into a list of NewsArticle objects."""
        return [cls.from_dict(item) for item in data_list]