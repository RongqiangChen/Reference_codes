def linear_spline(df,
                  x = None,
                  xt = None,
                  knots = None,
                  floor_cap = (-np.inf, np.inf)
                  valid_range = (-np.inf, np.inf)
                 ):
  """ Linear spline function
  param df: Spark df
  type df: Spark df
  param x: variable names to be linear spline transformed
  type x: string
  param xt: variable name prefix for post linear spline transformed
  type xt: string
  param knots: list of numerical numbers used for spline
  type knots: list/tuple
  param floor_cap: floor/cap knots to flatten out
  type floor_cap: list/tuple
  param valid_range: if x is outside valid range, it will be treated as missing
  type valid_range: list/tuple
  
  return: spark df
  
  """
  
  if (isinstance(x, str) is False) or (x is None):
    raise ValueError("variable name should be a non-empty string")
    
  x_in_df = True if x in df.columns else False
  
  if xt is None:
    xt = x
  elif isinstance(xt, str) is False:
    raise ValueError("prefix name should be a non-empty string")
    
  if isinstance(knots, (tuple, list)) is False:
    raise valueError("knots should be a non-empty tuple or list")
    
  nknots = len(knots)
  
  if floor_cap is not None and not (isinstance(floor_cap, (tuple, list)) and len(floor_cap) == 2):
    raise ValueError("knots floor cap should be None or length 2 tuple or list")
    
  if floor_cap and len(floor_cap) == 2:
    floor_value = floor_cap[0] if floor_cap[0] is not None else -np.inf
    cap_value = floor_cap[1] if floor_cap[1] is not None else np.inf
  else:
    floor_value = -np.inf
    cap_value = np.inf
    
  if valid_range is not None and not (isinstance(valid_range, (tuple, list)) and len(valid_range) == 2):
    raise ValueError("knots valid_range should be None or length 2 tuple or list")
    
  if valid_range and len(valid_range) == 2:
    floor_valid = valid_range[0] if valid_range[0] is not None else -np.inf
    cap_valid = valid_range[1] if valid_range[1] is not None else np.inf
  else:
    floor_valid = -np.inf
    cap_valid = np.inf  
  
#   temporarily store x

  df = df.withColumn( x + 'x_raw', expr(x))
  
  df = df.withColumn(x, when((expr(x) <= cap_valid) & (expr(x) >= floor_valid), expr(x)).otherwise(None))
  
  df = df.withColumn(xt + str(0), when(isnull(col(x)), 1).otherwise(0))
  
  
  
