import pandas as pd
import numpy as np
import regex as re
import pandarallel as parallel
from polyfuzz import PolyFuzz
from polyfuzz.models import EditDistance, TFIDF, RapidFuzz
import rapidfuzz
from rapidfuzz.process import extract, extractOne
from rapidfuzz import utils as fuzz_utils
from ftfy import fix_text


def cleaner(string) -> str | None:
    if string is not np.nan:
        string = string.strip()
        string = re.sub(r'\s{2,}', ' ', string)
        return string


def brand_cleaner(string, listObj):
    for item in listObj:
        if isinstance(item, dict):
            # string = string.replace(item.get('incorrect'), item.get('correct'))
            string = re.sub(item.get('incorrect'), item.get(
                'correct'), string, flags=re.I)
            string = string.replace('/', ' / ')

        elif isinstance(item, dict) == False:
            string = re.sub(r'|'.join(listObj), '', string, flags=re.I)
            string = string.replace('/', ' / ')
            return string.strip()

    return string.strip()


def differ(x):
    diffs = list(set(x['title_amz'].lower().split()).difference(
        x['title_wal'].lower().split()))
    if len(diffs) > 0:
        return diffs


def polymatch(list_from, list_to, engine=None):
    pass


def rapid_match(string, listObj, **kwargs):
    # scorer = kwargs.get('scorer', None)
    matcher = rapidfuzz.process.extractOne(string, listObj, **kwargs)
    df = pd.DataFrame(matcher, columns=['string', 'match', 'score'])
    return df


def multi_tri_merge(*args, **kwargs):
    if len(args) <= 2:
        return pd.merge(args[0], args[1], **kwargs)
    elif len(args) > 2:
        return pd.merge(args[0], args[1], **kwargs).merge(args[2], **kwargs)


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


def fuzzyFrame(correctFrame: pd.DataFrame, compareFrame: pd.DataFrame, onCol: str, mapKey: str, processor=fuzz_utils.default_process, scorer=rapidfuzz.fuzz.ratio, **kwargs) -> pd.DataFrame:
    """
    fuzzyFrame: merges dataframes on fuzzy matches

    Args:
        correctFrame (pd.DataFrame): DataFrame to match to
        compareFrame (pd.DataFrame): DataFrame to match values from
        onCol (pd.Series): Series to merge matches on
        mapKey (pd.Series): key mapped from dictionary with corresponding values

    Returns:
        pd.DataFrame: DataFrame with fuzzy matches, score & mapping
    """

    mapping = {key: fuzz_utils.default_process(value) for key, value in dict(
        compareFrame[[mapKey, onCol]].values).items()}
    # frame_map = dict(compareFrame[[mapKey, onCol]].values)
    # mapping = {key: fuzz_utils.default_process(key) for key in compareFrame[onCol]}

    correctFrame[[f'{onCol}_match', 'score', f'{mapKey}']] = correctFrame.parallel_apply(lambda x: rapidfuzz.process.extractOne(x[onCol], mapping,
                                                                                                                                processor=processor, scorer=scorer), axis=1, result_type='expand')
    onKey = mapKey
    # scorer=scorer, processor=process_string
    dfMerge = pd.merge(correctFrame, compareFrame, on=onKey,
                       **kwargs).sort_values(by='score')
    return dfMerge


def update_sheet(path, df, *args, **kwargs):
    """
    update_sheet --updates existing excel sheet

    Args:
        path (str): path to excel file
        df (pd.DataFrame): dataframe used to update excelfile
    """

    with pd.ExcelWriter(path, mode=kwargs.get('mode', 'a'),
                        engine=kwargs.get('engine', 'openpyxl'),
                        if_sheet_exists=kwargs.get('if_sheet_exists', 'overlay')) as writer:

        df.to_excel(writer, sheet_name=kwargs.get(
            'sheet_name', 'Sheet1'), index=kwargs.get('index', False))
