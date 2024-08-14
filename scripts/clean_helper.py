import regex as re
import numpy as np
from ftfy import fix_text


def cleaner(string: str) -> str | None:
    if string is not np.nan:
        string = string.strip()
        string = re.sub(r'\s{2,}', ' ', string)
        return string


def preprocess(string, stopwords: list = None):
    """
    preprocess - string cleaning

    Args:
        string (str): string to clean
        stopwords (list): list of stopwords to remove. default is None

    Returns:
        pd.Series: cleaned strings
    """

    to_remove = ['\(', '\)', ':', '\*', '\+', '\?',
                 '{', '}', '\[', '\]', '\^', '\$', '\|', '/', '\\', '=', '!', '~', '`', '#', '%', '&amp;', ';']
    string = re.sub(r'|'.join(to_remove), ' ', string)
    string = string.strip()
    if stopwords:
        string = re.sub(r'\b|\b'.join(stopwords), '', string, flags=re.I)
    string = fix_text(string)
    string = re.sub(r'\s{2,}', ' ', string)

    return string.strip()


# <-----------------------NO IDEA WHAT THIS FUNCTION WAS FOR ORIGINALLY----------------------->
# def brand_cleaner(string, listObj):
#     for item in listObj:
#         if isinstance(item, dict):
#             # string = string.replace(item.get('incorrect'), item.get('correct'))
#             string = re.sub(item.get('incorrect'), item.get(
#                 'correct'), string, flags=re.I)
#             string = string.replace('/', ' / ')

#         elif isinstance(item, dict) == False:
#             string = re.sub(r'|'.join(listObj), '', string, flags=re.I)
#             string = string.replace('/', ' / ')
#             return string.strip()

#     return string.strip()
