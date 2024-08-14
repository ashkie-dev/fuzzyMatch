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


def differ(x):
    diffs = list(set(x['title_amz'].lower().split()).difference(
        x['title_wal'].lower().split()))
    if len(diffs) > 0:
        return diffs


def polymatch(list_from, list_to, engine=None):
    pass


def rapid_match(string: str, listObj: list, **kwargs):
    # scorer = kwargs.get('scorer', None)
    matcher = rapidfuzz.process.extractOne(string, listObj, **kwargs)
    df = pd.DataFrame(matcher, columns=['string', 'match', 'score'])
    return df


def multi_tri_merge(*args, **kwargs):
    if len(args) <= 2:
        return pd.merge(args[0], args[1], **kwargs)
    elif len(args) > 2:
        return pd.merge(args[0], args[1], **kwargs).merge(args[2], **kwargs)


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

    correctFrame[[f'{onCol}_match', 'score', f'{mapKey}']] = correctFrame.parallel_apply(lambda x: extractOne(x[onCol], mapping,
                                                                                                              processor=processor, scorer=scorer), axis=1, result_type='expand')
    onKey = mapKey
    # scorer=scorer, processor=process_string
    dfMerge = pd.merge(correctFrame, compareFrame, on=onKey,
                       **kwargs).sort_values(by='score')
    return dfMerge
