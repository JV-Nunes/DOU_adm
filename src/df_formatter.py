### Formatting DataFrames ###

def filter_by_prefix(str_list, prefix):
    """
    Return a list with the elements of 
    `str_list` (list of str) that start
    with `prefix` (str).
    """
    
    filtered = list(filter(lambda s: s[:len(prefix)] == prefix, str_list))
    
    return filtered


def build_prefix_fmt_dict(all_cols, prefix, value_fmt):
    """
    Build a Pandas styler dict (column name -> format)
    for columns in `all_cols` (list of str) that start
    with `prefix` (str), setting they format as 
    specified in `value_fmt` (str), e.g.:
    '{:.1f}'    
    """
    
    # Select columns that start with prefix:
    sel_cols = filter_by_prefix(all_cols, prefix)
    
    # Build style format dict for these columns:
    fmt_dict = dict(zip(sel_cols, [value_fmt] * len(sel_cols)))
    
    return fmt_dict


def build_fmt_dict(all_cols, prefix_fmt_pairs):
    """
    Return a dict from column name to format string
    for all columns in `all_cols` according to 
    the prefixes and format strings specified as 
    pairs in the list of tuples `prefix_fmt-pairs`.
    """
    
    fmt_dict = dict()
    for prefix, fmt in prefix_fmt_pairs:
        fmt_dict.update(build_prefix_fmt_dict(all_cols, prefix, fmt))
        
    return fmt_dict


def build_neg_to_dash(fmt):
    """
    Given a string to be used to 
    format a number `fmt`, return 
    a function that will apply the 
    formatter to positive numbers 
    and return '-' for negative 
    numbers.
    """
    
    def formatter(x):
        """
        When `x` is a number, replace it 
        by a formatted str version if 
        `x >= 0` or return '-' otherwise.
        """
        
        # Security check for strings:
        if type(x) == str:
            return x
        
        # Format numbers:
        if x >= 0:
            return fmt.format(x)
        else:
            return '-'
    
    return formatter

def build_fmt_funcs(all_cols, prefix_fmt_pairs):
    """
    Build a dict from column name to 
    Pandas styler formatting function 
    where the columns are those in 
    `all_cols` (list of str) and the 
    functions replace negative numbers 
    by '-' and format the rest according 
    to the format string associated to 
    the prefix of column names listed in
    `prefix_fmt_pairs` (list of tuples).
    """
    
    # Build column to format dict:
    fmt_dict  = build_fmt_dict(all_cols, prefix_fmt_pairs)
    # Add negative to dash formatting:
    fmt_funcs = {k: build_neg_to_dash(v) for k,v in fmt_dict.items()}
    
    return fmt_funcs
