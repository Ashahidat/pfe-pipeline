# similarity.py
# Module complet pour calculer les similarités entre deux datasets Spark

import hashlib
import pandas as pd
import numpy as np
from difflib import SequenceMatcher
import os
import json

##########################################
# 1. STRUCTURE SIMILARITY
##########################################
def structure_similarity(df1, df2):
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)

    if len(cols1) == 0 or len(cols2) == 0:
        return 0.0

    # Overlap des colonnes
    overlap = len(cols1.intersection(cols2)) / max(len(cols1), len(cols2))

    # Similarité des types
    types1 = dict(df1.dtypes)
    types2 = dict(df2.dtypes)

    type_scores = []
    for c in cols1.intersection(cols2):
        t1 = str(types1[c])
        t2 = str(types2[c])
        type_scores.append(type_similarity(t1, t2))

    type_score = np.mean(type_scores) if type_scores else 0

    # Score final structure
    return 0.7 * overlap + 0.3 * type_score


def type_similarity(t1, t2):
    if t1 == t2:
        return 1.0
    if ("int" in t1 and "float" in t2) or ("float" in t1 and "int" in t2):
        return 0.8
    if ("string" in t1 and "int" in t2) or ("int" in t1 and "string" in t2):
        return 0.2
    return 0.5


##########################################
# 2. STATISTICS SIMILARITY
##########################################
def statistics_similarity(df1, df2):
    # Convertir Spark → Pandas (sample pour éviter heavy load)
    pdf1 = df1.sample(fraction=0.2).toPandas()
    pdf2 = df2.sample(fraction=0.2).toPandas()

    # Colonnes numériques communes
    common = [c for c in pdf1.columns if c in pdf2.columns and pd.api.types.is_numeric_dtype(pdf1[c])]

    if not common:
        return 0.0

    scores = []
    for c in common:
        v1 = pdf1[c].dropna()
        v2 = pdf2[c].dropna()
        if len(v1) == 0 or len(v2) == 0:
            continue

        stats1 = np.array([v1.mean(), v1.std(), v1.min(), v1.max()])
        stats2 = np.array([v2.mean(), v2.std(), v2.min(), v2.max()])

        # Distance euclidienne normalisée
        dist = np.linalg.norm(stats1 - stats2)
        score = 1 / (1 + dist)
        scores.append(score)

    return float(np.mean(scores)) if scores else 0.0


##########################################
# 3. SIZE SIMILARITY
##########################################
def size_similarity(df1, df2):
    n1 = df1.count()
    n2 = df2.count()

    if n1 == 0 or n2 == 0:
        return 0.0

    return min(n1, n2) / max(n1, n2)


##########################################
# 4. HASH PARTIAL SIMILARITY
##########################################
def hash_partial(df, n=1000):
    pdf = df.limit(n).toPandas()
    csv_str = pdf.to_csv(index=False)
    return hashlib.sha256(csv_str.encode()).hexdigest()


def hash_similarity(h1, h2):
    # Si identique → version exacte
    if h1 == h2:
        return 1.0

    # Score grossier basé sur similarité textuelle
    return SequenceMatcher(None, h1, h2).ratio()


##########################################
# 5. QUALITY TEST SIMILARITY (connecté à validation.json)
##########################################

def flatten_results(data):
    flat_results = []

    for category, tests in data.items():
        if isinstance(tests, list):
            for test in tests:
                test["category"] = category
                flat_results.append(test)
        elif isinstance(tests, dict):
            for subkey, subtests in tests.items():
                if isinstance(subtests, list):
                    for test in subtests:
                        test["category"] = f"{category}.{subkey}"
                        flat_results.append(test)
    return flat_results


def summarize_quality(flat_results):
    summary = {}
    for test in flat_results:
        cat = test["category"]

        if "duplicates" in cat:
            summary["duplicates"] = test.get("nombre", 0)
        elif "regex" in cat:
            summary["regex_errors"] = test.get("errors", 0)
        elif "type" in cat:
            summary["type_errors"] = test.get("errors", 0)
        elif "missing" in cat:
            summary["missing_values"] = test.get("errors", 0)
    return summary


def quality_similarity(tests1: dict, tests2: dict):
    common = set(tests1.keys()).intersection(set(tests2.keys()))
    if not common:
        return 0.0

    scores = []
    for t in common:
        v1 = tests1[t]
        v2 = tests2[t]

        if isinstance(v1, bool):
            scores.append(1.0 if v1 == v2 else 0.0)
        else:
            scores.append(1 / (1 + abs(v1 - v2)))

    return float(np.mean(scores))

##########################################
# 6. FINAL COMPOSITE SCORE
##########################################
def global_similarity(df1, df2, hash1, hash2, tests1, tests2):
    s1 = structure_similarity(df1, df2)
    s2 = statistics_similarity(df1, df2)
    s3 = size_similarity(df1, df2)
    s4 = hash_similarity(hash1, hash2)
    s5 = quality_similarity(tests1, tests2)

    final = (
        0.35 * s1 +
        0.25 * s2 +
        0.15 * s3 +
        0.15 * s4 +
        0.10 * s5
    )

    return float(final)

##########################################
# 7. UTILITÉ : charger résultats validation.json
##########################################
def load_quality_results(result_file):
    if not os.path.exists(result_file):
        return {}

    with open(result_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    flat = flatten_results(data)
    summary = summarize_quality(flat)
    return summary
