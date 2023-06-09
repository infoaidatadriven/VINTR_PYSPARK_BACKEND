### Original Author: https://github.com/shakedzy/dython
import numpy as np
import pandas as pd
import math
import warnings
from collections import Counter
import scipy.stats as ss


def convert(data, to):
    converted = None
    if to == 'array':
        if isinstance(data, np.ndarray):
            converted = data
        elif isinstance(data, pd.Series):
            converted = data.values
        elif isinstance(data, list):
            converted = np.array(data)
        elif isinstance(data, pd.DataFrame):
            converted = data.as_matrix()
    elif to == 'list':
        if isinstance(data, list):
            converted = data
        elif isinstance(data, pd.Series):
            converted = data.values.tolist()
        elif isinstance(data, np.ndarray):
            converted = data.tolist()
    elif to == 'dataframe':
        if isinstance(data, pd.DataFrame):
            converted = data
        elif isinstance(data, np.ndarray):
            converted = pd.DataFrame(data)
    else:
        raise ValueError("Unknown data conversion: {}".format(to))
    if converted is None:
        raise TypeError(
            'cannot handle data conversion of type: {} to {}'.format(
                type(data), to))
    else:
        return converted

def remove_incomplete_samples(x, y):
    x = [v if v is not None else np.nan for v in x]
    y = [v if v is not None else np.nan for v in y]
    arr = np.array([x, y]).transpose()
    arr = arr[~np.isnan(arr).any(axis=1)].transpose()
    if isinstance(x, list):
        return arr[0].tolist(), arr[1].tolist()
    else:
        return arr[0], arr[1]


def replace_nan_with_value(x, y, value):
    x = np.array([v if v == v and v is not None else value for v in x])  # NaN != NaN
    y = np.array([v if v == v and v is not None else value for v in y])
    return x, y


_REPLACE = 'replace'
_DROP = 'drop'
_DROP_SAMPLES = 'drop_samples'
_DROP_FEATURES = 'drop_features'
_SKIP = 'skip'
_DEFAULT_REPLACE_VALUE = 0.0


def _inf_nan_str(x):
    if np.isnan(x):
        return 'NaN'
    elif abs(x) == np.inf:
        return 'inf'
    else:
        return ''


def conditional_entropy(x,
                        y,
                        nan_strategy=_REPLACE,
                        nan_replace_value=_DEFAULT_REPLACE_VALUE,
                        log_base: float = math.e):
    """
    Calculates the conditional entropy of x given y: S(x|y)
    Wikipedia: https://en.wikipedia.org/wiki/Conditional_entropy
    Parameters:
    -----------
    x : list / NumPy ndarray / Pandas Series
        A sequence of measurements
    y : list / NumPy ndarray / Pandas Series
        A sequence of measurements
    nan_strategy : string, default = 'replace'
        How to handle missing values: can be either 'drop' to remove samples
        with missing values, or 'replace' to replace all missing values with
        the nan_replace_value. Missing values are None and np.nan.
    nan_replace_value : any, default = 0.0
        The value used to replace missing values with. Only applicable when
        nan_strategy is set to 'replace'.
    log_base: float, default = e
        specifying base for calculating entropy. Default is base e.
    Returns:
    --------
    float
    """
    if nan_strategy == _REPLACE:
        x, y = replace_nan_with_value(x, y, nan_replace_value)
    elif nan_strategy == _DROP:
        x, y = remove_incomplete_samples(x, y)
    y_counter = Counter(y)
    xy_counter = Counter(list(zip(x, y)))
    total_occurrences = sum(y_counter.values())
    entropy = 0.0
    for xy in xy_counter.keys():
        p_xy = xy_counter[xy] / total_occurrences
        p_y = y_counter[xy[1]] / total_occurrences
        entropy += p_xy * math.log(p_y / p_xy, log_base)
    return entropy


def cramers_v(x,
              y,
              bias_correction=True,
              nan_strategy=_REPLACE,
              nan_replace_value=_DEFAULT_REPLACE_VALUE):
    """
    Calculates Cramer's V statistic for categorical-categorical association.
    This is a symmetric coefficient: V(x,y) = V(y,x)
    Original function taken from: https://stackoverflow.com/a/46498792/5863503
    Wikipedia: https://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V
    Parameters:
    -----------
    x : list / NumPy ndarray / Pandas Series
        A sequence of categorical measurements
    y : list / NumPy ndarray / Pandas Series
        A sequence of categorical measurements
    bias_correction : Boolean, default = True
        Use bias correction from Bergsma and Wicher,
        Journal of the Korean Statistical Society 42 (2013): 323-328.
    nan_strategy : string, default = 'replace'
        How to handle missing values: can be either 'drop' to remove samples
        with missing values, or 'replace' to replace all missing values with
        the nan_replace_value. Missing values are None and np.nan.
    nan_replace_value : any, default = 0.0
        The value used to replace missing values with. Only applicable when
        nan_strategy is set to 'replace'.
    Returns:
    --------
    float in the range of [0,1]
    """
    if nan_strategy == _REPLACE:
        x, y = replace_nan_with_value(x, y, nan_replace_value)
    elif nan_strategy == _DROP:
        x, y = remove_incomplete_samples(x, y)
    confusion_matrix = pd.crosstab(x, y)
    chi2 = ss.chi2_contingency(confusion_matrix)[0]
    n = confusion_matrix.sum().sum()
    phi2 = chi2 / n
    r, k = confusion_matrix.shape
    if bias_correction:
        phi2corr = max(0, phi2 - ((k - 1) * (r - 1)) / (n - 1))
        rcorr = r - ((r - 1) ** 2) / (n - 1)
        kcorr = k - ((k - 1) ** 2) / (n - 1)
        if min((kcorr - 1), (rcorr - 1)) == 0:
            warnings.warn(
                "Unable to calculate Cramer's V using bias correction. Consider using bias_correction=False",
                RuntimeWarning)
            return np.nan
        else:
            return np.sqrt(phi2corr / min((kcorr - 1), (rcorr - 1)))
    else:
        return np.sqrt(phi2 / min(k - 1, r - 1))


def theils_u(x,
             y,
             nan_strategy=_REPLACE,
             nan_replace_value=_DEFAULT_REPLACE_VALUE):
    """
    Calculates Theil's U statistic (Uncertainty coefficient) for categorical-
    categorical association. This is the uncertainty of x given y: value is
    on the range of [0,1] - where 0 means y provides no information about
    x, and 1 means y provides full information about x.
    This is an asymmetric coefficient: U(x,y) != U(y,x)
    Wikipedia: https://en.wikipedia.org/wiki/Uncertainty_coefficient
    Parameters:
    -----------
    x : list / NumPy ndarray / Pandas Series
        A sequence of categorical measurements
    y : list / NumPy ndarray / Pandas Series
        A sequence of categorical measurements
    nan_strategy : string, default = 'replace'
        How to handle missing values: can be either 'drop' to remove samples
        with missing values, or 'replace' to replace all missing values with
        the nan_replace_value. Missing values are None and np.nan.
    nan_replace_value : any, default = 0.0
        The value used to replace missing values with. Only applicable when
        nan_strategy is set to 'replace'.
    Returns:
    --------
    float in the range of [0,1]
    """
    if nan_strategy == _REPLACE:
        x, y = replace_nan_with_value(x, y, nan_replace_value)
    elif nan_strategy == _DROP:
        x, y = remove_incomplete_samples(x, y)
    s_xy = conditional_entropy(x, y)
    x_counter = Counter(x)
    total_occurrences = sum(x_counter.values())
    p_x = list(map(lambda n: n / total_occurrences, x_counter.values()))
    s_x = ss.entropy(p_x)
    if s_x == 0:
        return 1
    else:
        return (s_x - s_xy) / s_x


def correlation_ratio(categories,
                      measurements,
                      nan_strategy=_REPLACE,
                      nan_replace_value=_DEFAULT_REPLACE_VALUE):
    """
    Calculates the Correlation Ratio (sometimes marked by the greek letter Eta)
    for categorical-continuous association.
    Answers the question - given a continuous value of a measurement, is it
    possible to know which category is it associated with?
    Value is in the range [0,1], where 0 means a category cannot be determined
    by a continuous measurement, and 1 means a category can be determined with
    absolute certainty.
    Wikipedia: https://en.wikipedia.org/wiki/Correlation_ratio
    Parameters:
    -----------
    categories : list / NumPy ndarray / Pandas Series
        A sequence of categorical measurements
    measurements : list / NumPy ndarray / Pandas Series
        A sequence of continuous measurements
    nan_strategy : string, default = 'replace'
        How to handle missing values: can be either 'drop' to remove samples
        with missing values, or 'replace' to replace all missing values with
        the nan_replace_value. Missing values are None and np.nan.
    nan_replace_value : any, default = 0.0
        The value used to replace missing values with. Only applicable when
        nan_strategy is set to 'replace'.
    Returns:
    --------
    float in the range of [0,1]
    """
    if nan_strategy == _REPLACE:
        categories, measurements = replace_nan_with_value(
            categories, measurements, nan_replace_value)
    elif nan_strategy == _DROP:
        categories, measurements = remove_incomplete_samples(
            categories, measurements)
    categories = convert(categories, 'array')
    measurements = convert(measurements, 'array')
    fcat, _ = pd.factorize(categories)
    cat_num = np.max(fcat) + 1
    y_avg_array = np.zeros(cat_num)
    n_array = np.zeros(cat_num)
    for i in range(0, cat_num):
        cat_measures = measurements[np.argwhere(fcat == i).flatten()]
        n_array[i] = len(cat_measures)
        y_avg_array[i] = np.average(cat_measures)
    y_total_avg = np.sum(np.multiply(y_avg_array, n_array)) / np.sum(n_array)
    numerator = np.sum(
        np.multiply(n_array, np.power(np.subtract(y_avg_array, y_total_avg),
                                      2)))
    denominator = np.sum(np.power(np.subtract(measurements, y_total_avg), 2))
    if numerator == 0:
        eta = 0.0
    else:
        eta = np.sqrt(numerator / denominator)
    return eta

def identify_columns_by_type(dataset, include):
    """
    Given a dataset, identify columns of the types requested.
    Parameters:
    -----------
    dataset : NumPy ndarray / Pandas DataFrame
    include : list of strings
        Desired column types
    Returns:
    --------
    A list of columns names
    Example:
    --------
    >> df = pd.DataFrame({'col1': ['a', 'b', 'c', 'a'], 'col2': [3, 4, 2, 1], 'col3': [1., 2., 3., 4.]})
    >> identify_columns_by_type(df, include=['int64', 'float64'])
    ['col2', 'col3']
    """
    dataset = convert(dataset, 'dataframe')
    columns = list(dataset.select_dtypes(include=include).columns)
    return columns

def identify_nominal_columns(dataset):
    """
    Given a dataset, identify categorical columns.
    Parameters:
    -----------
    dataset : NumPy ndarray / Pandas DataFrame
    Returns:
    --------
    A list of categorical columns names
    Example:
    --------
    >> df = pd.DataFrame({'col1': ['a', 'b', 'c', 'a'], 'col2': [3, 4, 2, 1]})
    >> identify_nominal_columns(df)
    ['col1']
    """
    return identify_columns_by_type(dataset, include=['object', 'category'])


def identify_numeric_columns(dataset):
    """
    Given a dataset, identify numeric columns.
    Parameters:
    -----------
    dataset : NumPy ndarray / Pandas DataFrame
    Returns:
    --------
    A list of numerical columns names
    Example:
    --------
    >> df = pd.DataFrame({'col1': ['a', 'b', 'c', 'a'], 'col2': [3, 4, 2, 1], 'col3': [1., 2., 3., 4.]})
    >> identify_numeric_columns(df)
    ['col2', 'col3']
    """
    return identify_columns_by_type(dataset, include=['int64', 'float64'])


def _comp_assoc(dataset, nominal_columns, mark_columns, theil_u, clustering,
                bias_correction, nan_strategy, nan_replace_value):
    """
    This is a helper function for compute_associations and associations
    """
    dataset = convert(dataset, 'dataframe')
    if nan_strategy == _REPLACE:
        dataset.fillna(nan_replace_value, inplace=True)
    elif nan_strategy == _DROP_SAMPLES:
        dataset.dropna(axis=0, inplace=True)
    elif nan_strategy == _DROP_FEATURES:
        dataset.dropna(axis=1, inplace=True)
    columns = dataset.columns
    if nominal_columns is None:
        nominal_columns = list()
    elif nominal_columns == 'all':
        nominal_columns = columns
    elif nominal_columns == 'auto':
        nominal_columns = identify_nominal_columns(dataset)

    corr = pd.DataFrame(index=columns, columns=columns)
    single_value_columns = []
    inf_nan = pd.DataFrame(data=np.zeros_like(corr),
                           columns=columns,
                           index=columns)
    for c in columns:
        if dataset[c].unique().size == 1:
            single_value_columns.append(c)
    for i in range(0, len(columns)):
        if columns[i] in single_value_columns:
            corr.loc[:, columns[i]] = 0.0
            corr.loc[columns[i], :] = 0.0
            continue
        for j in range(i, len(columns)):
            if columns[j] in single_value_columns:
                continue
            elif i == j:
                corr.loc[columns[i], columns[j]] = 1.0
            else:
                if columns[i] in nominal_columns:
                    if columns[j] in nominal_columns:
                        if theil_u:
                            ji = theils_u(
                                dataset[columns[i]],
                                dataset[columns[j]],
                                nan_strategy=_SKIP)
                            ij = theils_u(
                                dataset[columns[j]],
                                dataset[columns[i]],
                                nan_strategy=_SKIP)
                        else:
                            cell = cramers_v(dataset[columns[i]],
                                             dataset[columns[j]],
                                             bias_correction=bias_correction,
                                             nan_strategy=_SKIP)
                            ij = cell
                            ji = cell
                    else:
                        cell = correlation_ratio(dataset[columns[i]],
                                                 dataset[columns[j]],
                                                 nan_strategy=_SKIP)
                        ij = cell
                        ji = cell
                else:
                    if columns[j] in nominal_columns:
                        cell = correlation_ratio(dataset[columns[j]],
                                                 dataset[columns[i]],
                                                 nan_strategy=_SKIP)
                        ij = cell
                        ji = cell
                    else:
                        cell, _ = ss.pearsonr(dataset[columns[i]],
                                              dataset[columns[j]])
                        ij = cell
                        ji = cell
                corr.loc[columns[i], columns[j]] = ij if not np.isnan(ij) and abs(ij) < np.inf else 0.0
                corr.loc[columns[j], columns[i]] = ji if not np.isnan(ji) and abs(ji) < np.inf else 0.0
                inf_nan.loc[columns[i], columns[j]] = _inf_nan_str(ij)
                inf_nan.loc[columns[j], columns[i]] = _inf_nan_str(ji)
    corr.fillna(value=np.nan, inplace=True)
    if mark_columns:
        marked_columns = [
            '{} (nom)'.format(col)
            if col in nominal_columns else '{} (con)'.format(col)
            for col in columns
        ]
        corr.columns = marked_columns
        corr.index = marked_columns
        inf_nan.columns = marked_columns
        inf_nan.index = marked_columns
    if clustering:
        corr, _ = cluster_correlations(corr)
        columns = corr.columns
    return corr, columns, nominal_columns, inf_nan, single_value_columns


def compute_associations(dataset,
                         nominal_columns='auto',
                         mark_columns=False,
                         theil_u=False,
                         clustering=False,
                         bias_correction=True,
                         nan_strategy=_REPLACE,
                         nan_replace_value=_DEFAULT_REPLACE_VALUE,
                         ):
    """
    Calculate the correlation/strength-of-association of features in data-set
    with both categorical and continuous features using:
     * Pearson's R for continuous-continuous cases
     * Correlation Ratio for categorical-continuous cases
     * Cramer's V or Theil's U for categorical-categorical cases
    It is equivalent to run `associations(data, plot=False, ...)['corr']`, only
    it skips entirely on the drawing phase of the heat-map (See
    https://github.com/shakedzy/dython/issues/49)
    Parameters:
    -----------
    dataset : NumPy ndarray / Pandas DataFrame
        The data-set for which the features' correlation is computed
    nominal_columns : string / list / NumPy ndarray
        Names of columns of the data-set which hold categorical values. Can
        also be the string 'all' to state that all columns are categorical,
        'auto' (default) to try to identify nominal columns, or None to state
        none are categorical
    mark_columns : Boolean, default = False
        if True, output's columns' names will have a suffix of '(nom)' or
        '(con)' based on there type (eda_tools or continuous), as provided
        by nominal_columns
    theil_u : Boolean, default = False
        In the case of categorical-categorical feaures, use Theil's U instead
        of Cramer's V
    clustering : Boolean, default = False
        If True, hierarchical clustering is applied in order to sort
        features into meaningful groups
    bias_correction : Boolean, default = True
        Use bias correction for Cramer's V from Bergsma and Wicher,
        Journal of the Korean Statistical Society 42 (2013): 323-328.
    nan_strategy : string, default = 'replace'
        How to handle missing values: can be either 'drop_samples' to remove
        samples with missing values, 'drop_features' to remove features
        (columns) with missing values, or 'replace' to replace all missing
        values with the nan_replace_value. Missing values are None and np.nan.
    nan_replace_value : any, default = 0.0
        The value used to replace missing values with. Only applicable when
        nan_strategy is set to 'replace'
    Returns:
    --------
    A DataFrame of the correlation/strength-of-association between all features
    """
    corr, _, _, _, _ = _comp_assoc(dataset, nominal_columns, mark_columns, theil_u, clustering,
                                   bias_correction, nan_strategy, nan_replace_value)
    return corr
